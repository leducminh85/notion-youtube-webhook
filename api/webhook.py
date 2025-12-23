from flask import Flask, request, jsonify
import json
import requests
import os

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    # === LOG TOÀN BỘ PAYLOAD ĐỂ DEBUG ===
    print("=== Headers ===")
    print(request.headers)
    
    try:
        payload = request.json
        print("=== Payload JSON ===")
        print(json.dumps(payload, indent=4, ensure_ascii=False))
    except json.JSONDecodeError:
        print("=== Raw Body (không phải JSON) ===")
        print(request.data.decode('utf-8'))
        return jsonify({"status": "error", "message": "Invalid JSON"}), 400

    try:
        data = payload.get('data', {})
        props = data.get('properties', {})

        def get_property_value(prop_name, type_hint='text'):
            prop = props.get(prop_name)
            if prop is None:
                return None

            # Case A: flattened string
            if isinstance(prop, str):
                return prop

            # Case Postman-mock: { text: { content } }
            if prop.get('text', {}).get('content'):
                return prop['text']['content']

            # Real Notion: rich_text
            if 'rich_text' in prop and len(prop['rich_text']) > 0:
                value = ''
                for segment in prop['rich_text']:
                    if 'text' in segment and 'content' in segment['text']:
                        value += segment['text']['content']
                return value if value else None

            # Real Notion: title
            if 'title' in prop and len(prop['title']) > 0:
                value = ''
                for segment in prop['title']:
                    if 'text' in segment and 'content' in segment['text']:
                        value += segment['text']['content']
                return value if value else None

            # URL
            if 'url' in prop:
                return prop['url']

            return None

        yt_api_key = get_property_value('YouTube API Key')
        notion_api_key = get_property_value('Notion API Key')
        channel_url = get_property_value('YouTube Channel URL')
        page_id = get_property_value('page_id')

        if not all([yt_api_key, notion_api_key, channel_url, page_id]):
            raise ValueError('Missing required properties: YouTube API Key, Notion API Key, Channel URL, or page_id')

        # Bước 1: Lấy Channel ID từ URL
        channel_id = None
        if '/channel/' in channel_url:
            channel_id = channel_url.split('/channel/')[1].split('?')[0]
        elif '/@' in channel_url:
            handle = channel_url.split('/@')[1].split('?')[0]
            channels_url = f"https://www.googleapis.com/youtube/v3/channels?forHandle={handle}&key={yt_api_key}&part=id"
            channels_response = requests.get(channels_url)
            channels_data = channels_response.json()
            if not channels_data.get('items') or len(channels_data['items']) == 0:
                raise ValueError('Channel not found by handle')
            channel_id = channels_data['items'][0]['id']
        else:
            raise ValueError('Invalid channel URL format')

        # Bước 2: Lấy Uploads Playlist ID
        channel_url = f"https://www.googleapis.com/youtube/v3/channels?id={channel_id}&key={yt_api_key}&part=contentDetails"
        channel_response = requests.get(channel_url)
        channel_data = channel_response.json()
        if not channel_data.get('items') or len(channel_data['items']) == 0:
            raise ValueError('Channel not found')
        uploads_playlist_id = channel_data['items'][0]['contentDetails']['relatedPlaylists']['uploads']

        # Bước 3: Lấy danh sách video (tối đa 100)
        videos = []
        next_page_token = ''
        while len(videos) < 100 and next_page_token is not None:
            url = f"https://www.googleapis.com/youtube/v3/playlistItems?playlistId={uploads_playlist_id}&key={yt_api_key}&part=snippet&maxResults=50&pageToken={next_page_token}"
            playlist_response = requests.get(url)
            playlist_data = playlist_response.json()
            filtered_items = [item for item in playlist_data.get('items', []) if item['snippet']['title'] not in ['Private video', 'Deleted video']]
            videos.extend(filtered_items)
            next_page_token = playlist_data.get('nextPageToken', None)

        # Bước 4: Tạo database mới trong Notion
        notion_headers = {
            'Authorization': f'Bearer {notion_api_key}',
            'Content-Type': 'application/json',
            'Notion-Version': '2022-06-28'
        }

        database_payload = {
            'parent': {'type': 'page_id', 'page_id': page_id},
            'title': [{'type': 'text', 'text': {'content': f'YouTube Videos - {channel_id}'}}],
            'properties': {
                'Title': {'title': {}},
                'Video URL': {'url': {}},
                'Published': {'date': {}},
                'Thumbnail': {'files': {}}  # Thay url thành files để thumbnail hiển thị đẹp hơn (external)
            }
        }

        create_db_response = requests.post('https://api.notion.com/v1/databases', headers=notion_headers, json=database_payload)
        new_db = create_db_response.json()
        new_db_id = new_db['id']

        # Bước 5: Thêm videos vào database
        for video in videos:
            snippet = video['snippet']
            video_payload = {
                'parent': {'database_id': new_db_id},
                'properties': {
                    'Title': {'title': [{'text': {'content': snippet['title']}}]},
                    'Video URL': {'url': f"https://www.youtube.com/watch?v={snippet['resourceId']['videoId']}"},
                    'Published': {'date': {'start': snippet['publishedAt']}},
                    'Thumbnail': {'files': [{'name': 'thumbnail', 'type': 'external', 'external': {'url': snippet['thumbnails'].get('high', {}).get('url') or snippet['thumbnails'].get('default', {}).get('url')}}]}
                }
            }

            requests.post('https://api.notion.com/v1/pages', headers=notion_headers, json=video_payload)

        # Trả về response thành công
        return jsonify({"status": "success", "message": "Webhook processed successfully!"}), 200

    except Exception as error:
        print(f'Error: {str(error)}')
        return jsonify({"status": "error", "message": str(error)}), 500

if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)