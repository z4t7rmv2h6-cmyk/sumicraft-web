"""SumiCraft v12: standard-library WSGI site + best-effort YouTube metadata.

Gunicorn runs this module's `app` callable on the web host. Visitors do not
need Python or an API key. YouTube itself may reject automated retrieval.
"""
from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus
import hashlib
import io
import json
import os
from pathlib import Path
import re
import threading
import time
import urllib.error
import urllib.request

APP_DIR = Path(__file__).resolve().parent
VIDEO_ID = re.compile(r'^[A-Za-z0-9_-]{11}$')
MAX_COMMENTS = 160
CACHE_SECONDS = 1200
CACHE: dict[str, tuple[float, dict]] = {}
CACHE_LOCK = threading.Lock()
RATE: dict[str, list[float]] = {}
RATE_LOCK = threading.Lock()
EXTRACTION_SLOTS = threading.BoundedSemaphore(2)


def optional_comment_downloader():
    try:
        from youtube_comment_downloader import YoutubeCommentDownloader, SORT_BY_POPULAR
        return YoutubeCommentDownloader, SORT_BY_POPULAR
    except ImportError:
        return None


def optional_yt_dlp():
    try:
        import yt_dlp
        return yt_dlp
    except ImportError:
        return None


def safe_text(value, limit=12000):
    return str(value if value is not None else '')[:limit]


def normalize_info(video_id: str, info: dict) -> dict:
    if info.get('id') != video_id:
        raise ValueError('取得した動画IDが一致しません。')
    channel_id = safe_text(info.get('channel_id'), 180)
    comments = []
    for row in (info.get('comments') or [])[:MAX_COMMENTS]:
        if not isinstance(row, dict):
            continue
        author_id = safe_text(row.get('author_id'), 180)
        comments.append({
            'id': safe_text(row.get('id'), 180),
            'author': safe_text(row.get('author'), 80),
            'authorId': author_id,
            'creator': bool(row.get('author_is_uploader')) or bool(channel_id and channel_id == author_id),
            'text': safe_text(row.get('text'), 12000),
        })
    return {
        'videoId': video_id,
        'title': safe_text(info.get('title') or f'参考動画 {video_id}', 120),
        'channelId': channel_id,
        'description': safe_text(info.get('description'), 120000),
        'comments': comments,
        'commentsChecked': len(comments),
        'commentsAvailable': info.get('comments') is not None,
        'fetchedAt': datetime.now(timezone.utc).isoformat(),
    }


def extract_comments_without_player(video_id: str) -> dict:
    """Get public comments without resolving the YouTube video player.

    This can still be blocked by YouTube. Limit comments, size, and work time.
    """
    module = optional_comment_downloader()
    if module is None:
        raise RuntimeError('コメント直接取得ライブラリが利用できません。')
    Downloader, POPULAR = module
    downloader = Downloader()
    deadline = time.monotonic() + 60
    comments = []
    source = downloader.get_comments_from_url(
        f'https://www.youtube.com/watch?v={video_id}', sort_by=POPULAR)
    for row in source:
        if not isinstance(row, dict):
            continue
        comments.append({
            'id': safe_text(row.get('cid') or row.get('id'), 180),
            'author': safe_text(row.get('author'), 80),
            'authorId': '',
            'creator': False,
            'text': safe_text(row.get('text'), 12000),
        })
        if len(comments) >= MAX_COMMENTS or time.monotonic() > deadline:
            break
    if not comments:
        raise RuntimeError('公開コメントが取得できませんでした。')
    return {
        'videoId': video_id,
        'title': f'参考動画 {video_id}',
        'channelId': '',
        'description': '',
        'comments': comments,
        'commentsChecked': len(comments),
        'commentsAvailable': True,
        'source': 'direct-comments',
        'fetchedAt': datetime.now(timezone.utc).isoformat(),
    }


def extract_video(video_id: str) -> dict:
    direct_error = None
    try:
        return extract_comments_without_player(video_id)
    except Exception as exc:
        direct_error = str(exc)[:160]
        print(f'Comment direct fetch failed for {video_id}: {type(exc).__name__}', flush=True)
    module = optional_yt_dlp()
    if module is None:
        raise RuntimeError('YouTubeのコメントを取得できませんでした。現在YouTube側から取得が制限されている可能性があります。画面の一括貼り付けも利用できます。')
    # Never accept an arbitrary URL. Construct only a canonical youtube URL.
    url = f'https://www.youtube.com/watch?v={video_id}'
    opts = {
        'quiet': True, 'no_warnings': True, 'skip_download': True,
        'getcomments': True, 'ignore_no_formats_error': True,
        'noplaylist': True, 'cachedir': False,
        'socket_timeout': 12, 'retries': 1, 'extractor_retries': 1,
        'extractor_args': {'youtube': {
            'max_comments': [str(MAX_COMMENTS), 'all', '0', '0'],
            'comment_sort': ['top'],
        }},
    }
    try:
        with module.YoutubeDL(opts) as downloader:
            info = downloader.extract_info(url, download=False)
    except Exception as exc:
        # Do not leak internal stack traces or filesystem paths.
        raise RuntimeError('YouTubeのコメント取得が2つの方法で失敗しました。公開サーバーからのアクセスが制限されている可能性があります。概要欄・コメントの文章を丸ごと貼り付ける機能は使えます。') from exc
    if not isinstance(info, dict):
        raise RuntimeError('動画情報を取得できませんでした。')
    return normalize_info(video_id, info)


class BusyError(Exception):
    pass


def cached_extract(video_id: str) -> dict:
    now = time.monotonic()
    with CACHE_LOCK:
        record = CACHE.get(video_id)
        if record and record[0] > now:
            return dict(record[1], cached=True)
    if not EXTRACTION_SLOTS.acquire(blocking=False):
        raise BusyError()
    try:
        data = extract_video(video_id)
    finally:
        EXTRACTION_SLOTS.release()
    with CACHE_LOCK:
        for key in [key for key, row in CACHE.items() if row[0] <= now]:
            CACHE.pop(key, None)
        if len(CACHE) > 150:
            CACHE.clear()
        CACHE[video_id] = (time.monotonic() + CACHE_SECONDS, data)
    return dict(data, cached=False)


def rate_ok(environ: dict) -> bool:
    # Transient only. No IP persistence or storage outside this process.
    ip = environ.get('HTTP_X_FORWARDED_FOR', '').split(',')[0].strip() or environ.get('REMOTE_ADDR', 'unknown')
    key = hashlib.sha256(('sumicraft-v11:' + ip).encode()).hexdigest()[:24]
    now = time.monotonic()
    with RATE_LOCK:
        times = [t for t in RATE.get(key, []) if now - t < 900]
        if len(times) >= 10:
            return False
        times.append(now)
        RATE[key] = times
        if len(RATE) > 10000:
            RATE.clear()
    return True


def respond(start_response, status: int, body: bytes, ctype: str, cache='no-store'):
    start_response(f'{status} {HTTPStatus(status).phrase}', [
        ('Content-Type', ctype),
        ('Content-Length', str(len(body))),
        ('Cache-Control', cache),
        ('X-Content-Type-Options', 'nosniff'),
        ('Referrer-Policy', 'strict-origin-when-cross-origin'),
    ])
    return [body]


def json_response(start_response, status: int, data: dict):
    return respond(start_response, status, json.dumps(data, ensure_ascii=False).encode('utf-8'), 'application/json; charset=utf-8')



# Vanilla assets are loaded only from Mojang's public sample resource_pack.
# Do not accept arbitrary URLs. Keep a bounded in-memory cache per worker.
VANILLA_ROOT = 'https://raw.githubusercontent.com/Mojang/bedrock-samples/main/resource_pack/'
VANILLA_ALLOWED = re.compile(
    r'(?:blocks\.json|textures/(?:terrain_texture|item_texture)\.json|textures/(?:blocks|items)/[a-zA-Z0-9_.-]+\.png)\Z'
)
VANILLA_CACHE: dict[str, tuple[float, bytes, str]] = {}
VANILLA_LOCK = threading.Lock()

def get_vanilla_asset(subpath: str) -> tuple[bytes, str]:
    if len(subpath) > 150 or not VANILLA_ALLOWED.fullmatch(subpath):
        raise ValueError('Invalid vanilla asset path')
    now = time.monotonic()
    with VANILLA_LOCK:
        record = VANILLA_CACHE.get(subpath)
        if record and record[0] > now:
            return record[1], record[2]
    request = urllib.request.Request(
        VANILLA_ROOT + subpath,
        headers={'User-Agent': 'SumiCraft/1.0 (Minecraft Bedrock reference)', 'Accept': 'image/png, application/json'},
    )
    try:
        with urllib.request.urlopen(request, timeout=12) as response:
            limit = 650000 if subpath.endswith('.json') else 1100000
            data = response.read(limit + 1)
            if len(data) > limit or not data:
                raise RuntimeError('Texture reference file is unavailable')
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError('公式サンプル画像を取得できませんでした') from exc
    ctype = 'image/png' if subpath.endswith('.png') else 'application/json; charset=utf-8'
    with VANILLA_LOCK:
        if len(VANILLA_CACHE) >= 450:
            VANILLA_CACHE.clear()
        VANILLA_CACHE[subpath] = (now + 21600, data, ctype)
    return data, ctype

def app(environ, start_response):
    """WSGI callable for Gunicorn. Same origin serves UI and API."""
    method = environ.get('REQUEST_METHOD', 'GET')
    path = environ.get('PATH_INFO', '/')
    if path == '/' and method in ('GET','HEAD'):
        return respond(start_response, 200, (APP_DIR / 'index.html').read_bytes() if method == 'GET' else b'', 'text/html; charset=utf-8', 'no-cache')
    if path == '/catalog.js' and method in ('GET','HEAD'):
        asset = APP_DIR / 'catalog.js'
        if not asset.is_file():
            return json_response(start_response, 503, {'error': 'ブロック一覧を準備中です。'})
        return respond(start_response, 200, asset.read_bytes() if method == 'GET' else b'', 'application/javascript; charset=utf-8', 'public, max-age=3600')
    if path.startswith('/api/vanilla/') and method in ('GET','HEAD'):
        subpath = path[len('/api/vanilla/'):]
        try:
            if method == 'HEAD':
                if not VANILLA_ALLOWED.fullmatch(subpath):
                    return json_response(start_response, 400, {'error':'Invalid vanilla asset path'})
                return respond(start_response, 200, b'', 'image/png' if subpath.endswith('.png') else 'application/json; charset=utf-8', 'public, max-age=3600')
            data, ctype = get_vanilla_asset(subpath)
            return respond(start_response, 200, data, ctype, 'public, max-age=21600')
        except ValueError as exc:
            return json_response(start_response, 400, {'error':str(exc)})
        except RuntimeError as exc:
            return json_response(start_response, 502, {'error':str(exc)})
    if path == '/api/health' and method == 'GET':
        supported = optional_comment_downloader() is not None or optional_yt_dlp() is not None
        return json_response(start_response, 200, {
            'ok': True, 'extractorAvailable': supported, 'version': 'v12',
            'message': '取得プログラムの準備ができています。YouTubeからの取得成功は保証されません。' if supported else '自動取得機能を準備中です。',
        })
    if path == '/api/youtube/extract' and method == 'POST':
        if 'application/json' not in environ.get('CONTENT_TYPE', '').lower():
            return json_response(start_response, 400, {'error': 'JSON形式で送信してください。'})
        try:
            length = int(environ.get('CONTENT_LENGTH') or 0)
        except (ValueError, TypeError):
            length = 0
        if length < 2 or length > 4096:
            return json_response(start_response, 413, {'error': '入力データのサイズが不正です。'})
        try:
            data = json.loads(environ['wsgi.input'].read(length).decode('utf-8'))
        except (ValueError, UnicodeDecodeError):
            return json_response(start_response, 400, {'error': '無効なリクエストです。'})
        video_id = data.get('videoId') if isinstance(data, dict) else None
        if not isinstance(video_id, str) or not VIDEO_ID.fullmatch(video_id):
            return json_response(start_response, 400, {'error': '正しいYouTube動画URLを登録してね。'})
        if not rate_ok(environ):
            return json_response(start_response, 429, {'error': '短時間に取得できる回数を超えました。少し後で試してね。'})
        try:
            return json_response(start_response, 200, cached_extract(video_id))
        except BusyError:
            return json_response(start_response, 503, {'error': '現在ほかの動画を処理中です。もう一度試してね。'})
        except (RuntimeError, ValueError) as exc:
            return json_response(start_response, 502, {'error': str(exc)})
    return json_response(start_response, 404, {'error': 'ページが見つかりません。'})


if __name__ == '__main__':
    from wsgiref.simple_server import make_server
    port = int(os.environ.get('PORT', '8000'))
    print(f'SumiCraft v12 (local developer test) http://127.0.0.1:{port}/')
    with make_server('127.0.0.1', port, app) as server:
        server.serve_forever()