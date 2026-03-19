import os
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from langdetect import detect, LangDetectException

# --- CONFIGURATION ---
YOUTUBE_API_KEY = "placeholder"  # Paste your key here
TARGET_FILES = 50
COMMENTS_PER_FILE = 20  # 1 Main Comment + 19 Replies
OUTPUT_DIR = "thread_files"

START_DATE = "2025-02-01T00:00:00Z"
END_DATE = "2026-02-28T23:59:59Z"

KEYWORDS = [
    "patriotism", "patriots", "patriotic", "nationalism", "nationality", 
    "national", "nation", "xenophobia", "xenophobic", "fascism", 
    "country", "national identity", "#USA", "#America", "#Patriot", 
    "#ProgressivePatriot", "#Patriotic", "#UnitedStates", "#GodBlessAmerica", 
    "#StarsAndStripes", "#Freedom", "#Liberty", "#SupportOurTroops", 
    "#Veterans", "#HonorOurHeroes", "#MilitaryLife", "#HomeOfTheBrave", 
    "#MAGA", "#AmericaFirst", "#RedWhiteAndBlue", "#Constitution", "#2A", 
    "#WeThePeople", "#SilentMajority", "#AmericanMade", "#1776", 
    "#DonTreadOnMe", "#FaithAndCountry", "#PatriotNation", "#CountryOverParty", 
    "#OurAmerica"
]

def is_meaningful_english(text):
    """Filters out non-English, short, or spam comments."""
    # 1. Filter out spam links
    if "http://" in text or "https://" in text or "www." in text:
        return False
        
    # 2. Filter out extremely short comments (likely meaningless)
    words = text.split()
    if len(words) < 5:
        return False
        
    # 3. Detect Language (Requires English)
    try:
        lang = detect(text)
        if lang != 'en':
            return False
    except LangDetectException:
        # If it fails (e.g., text is purely emojis/punctuation), drop it
        return False
        
    return True

def extract_threads():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    youtube = build("youtube", "v3", developerKey=YOUTUBE_API_KEY)
    files_created = 0
    
    print(f"Starting extraction. Target: {TARGET_FILES} high-quality text files...")

    for keyword in KEYWORDS:
        if files_created >= TARGET_FILES:
            break
            
        print(f"\n--- Searching Keyword/Hashtag: '{keyword}' ---")
        next_page_token = None
        
        while files_created < TARGET_FILES:
            try:
                search_response = youtube.search().list(
                    q=keyword,
                    part="id",
                    type="video",
                    regionCode="US",
                    relevanceLanguage="en",
                    publishedAfter=START_DATE,
                    publishedBefore=END_DATE,
                    maxResults=50,
                    pageToken=next_page_token
                ).execute()
                
                if not search_response.get("items"):
                    break

                video_ids = [item["id"]["videoId"] for item in search_response["items"]]
                
                videos_response = youtube.videos().list(
                    part="snippet",
                    id=",".join(video_ids)
                ).execute()

                for video in videos_response.get("items", []):
                    if files_created >= TARGET_FILES:
                        break
                        
                    vid_id = video["id"]
                    vid_title = video["snippet"]["title"]
                    vid_desc = video["snippet"]["description"]
                    vid_url = f"https://www.youtube.com/watch?v={vid_id}"
                    
                    extracted_tags = re.findall(r"#\w+", vid_desc)
                    if not extracted_tags and keyword.startswith("#"):
                        extracted_tags = [keyword]
                    tags_string = " ".join(set(extracted_tags)) if extracted_tags else "None"
                    
                    try:
                        threads_response = youtube.commentThreads().list(
                            part="snippet",
                            videoId=vid_id,
                            maxResults=50,
                            textFormat="plainText",
                            order="relevance"
                        ).execute()
                        
                        for thread in threads_response.get("items", []):
                            if files_created >= TARGET_FILES:
                                break
                                
                            reply_count = thread["snippet"]["totalReplyCount"]
                            
                            # Only bother checking if it originally had 19+ replies
                            if reply_count >= (COMMENTS_PER_FILE - 1):
                                top_comment = thread["snippet"]["topLevelComment"]["snippet"]
                                main_author = top_comment["authorDisplayName"]
                                main_text = top_comment["textDisplay"].replace('\n', ' ')
                                
                                # Check if the main comment is valid English/Meaningful
                                if not is_meaningful_english(main_text):
                                    continue
                                
                                # Fetch up to 100 replies to filter down
                                replies_response = youtube.comments().list(
                                    part="snippet",
                                    parentId=thread["id"],
                                    maxResults=100, 
                                    textFormat="plainText"
                                ).execute()
                                
                                valid_replies_data = []
                                for reply in replies_response.get("items", []):
                                    reply_snippet = reply["snippet"]
                                    reply_author = reply_snippet["authorDisplayName"]
                                    reply_text = reply_snippet["textDisplay"].replace('\n', ' ')
                                    
                                    # Filter the reply
                                    if is_meaningful_english(reply_text):
                                        replying_to = main_author
                                        mention_match = re.match(r'^@([\w\.\-]+)', reply_text)
                                        if mention_match:
                                            replying_to = mention_match.group(1)
                                            
                                        valid_replies_data.append(f"{reply_author} replied to {replying_to}: {reply_text}")
                                        
                                    # Stop once we have exactly 19 valid replies
                                    if len(valid_replies_data) == (COMMENTS_PER_FILE - 1):
                                        break

                                # Only create the file if 19 valid replies survived the filter
                                if len(valid_replies_data) == (COMMENTS_PER_FILE - 1):
                                    files_created += 1
                                    filename = os.path.join(OUTPUT_DIR, f"thread_{files_created:02d}.txt")
                                    
                                    with open(filename, "w", encoding="utf-8") as f:
                                        f.write(f"Hashtags: {tags_string}\n")
                                        f.write(f"Video Link: {vid_url}\n")
                                        f.write(f"Video Title: {vid_title}\n")
                                        f.write("-" * 50 + "\n\n")
                                        f.write(f"{main_author} posted: {main_text}\n\n")
                                        
                                        for r_data in valid_replies_data:
                                            f.write(f"{r_data}\n\n")
                                            
                                    print(f"✅ Created {filename} ({files_created}/{TARGET_FILES}) - Passed Quality Filter")

                    except HttpError as e:
                        if e.resp.status == 403:
                            pass 
                        else:
                            print(f"Warning: API Error on video {vid_id}")

                next_page_token = search_response.get("nextPageToken")
                if not next_page_token:
                    break 

            except HttpError as e:
                print(f"API Error: {e}")
                break

    print(f"\n🎉 Extraction complete! Check the '{OUTPUT_DIR}' folder.")

if __name__ == "__main__":
    extract_threads()