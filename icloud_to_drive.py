import os
import asyncio
from playwright.async_api import async_playwright
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# --- CẤU HÌNH ---
LINK_CHIA_SE = "https://www.icloud.com/sharedalbum/vi-vi/#B29JtdOXm2KLgqV" 
SCOPES = ['https://www.googleapis.com/auth/drive.file']
TEMP_DIR = "./temp_downloads"
HISTORY_FILE = "download_history.txt" 
os.makedirs(TEMP_DIR, exist_ok=True)

# CON SỐ VÀNG CHO TỐC ĐỘ: 16 LUỒNG
NUM_WORKERS = 16 
# CÁC HÀM XỬ LÝ GOOGLE DRIVE
def get_drive_credentials():
    """Chỉ lấy chứng chỉ 1 lần duy nhất để cấp cho các thợ"""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

def upload_to_drive(drive_service, file_path, folder_id):
    file_name = os.path.basename(file_path)
    original_name = file_name.split('_', 1)[-1] if '_' in file_name else file_name
    
    file_metadata = {'name': original_name, 'parents': [folder_id]}
    media = MediaFileUpload(file_path, resumable=True)
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

# ĐỘI THỢ CHẠY NGẦM ĐA LUỒNG (CONSUMERS)
async def upload_worker(worker_id, creds, upload_queue, history_urls):
    drive_service = build('drive', 'v3', credentials=creds)
    
    while True:
        item = await upload_queue.get()
        if item is None: 
            break
            
        file_path, file_name, folder_id, file_url = item
        
        try:
            print(f"  [{worker_id}] Đang đẩy lên Drive: {file_name}...")
            await asyncio.to_thread(upload_to_drive, drive_service, file_path, folder_id)
            
            if os.path.exists(file_path):
                os.remove(file_path)
            with open(HISTORY_FILE, "a", encoding="utf-8") as f:
                f.write(file_url + "\n")
            history_urls.add(file_url)
                
            print(f"  [{worker_id}] HOÀN TẤT & XÓA TẠM: {file_name}")
            
        except Exception as e:
            print(f"  [{worker_id}] LỖI {file_name} (Sẽ tải lại ở lần sau): {e}")
            
        finally:
            upload_queue.task_done()

# CÀO DỮ LIỆU ICLOUD (PRODUCER)
async def crawl_and_upload(shared_link, creds, photos_folder_id, videos_folder_id, skip_count):
    history_urls = set()
    if os.path.exists(HISTORY_FILE):
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            history_urls = set(line.strip().split('?')[0] for line in f if line.strip())
    print(f"[*] Đã tải thành công trước đó: {len(history_urls)} file (Dựa trên URL gốc).")

    upload_queue = asyncio.Queue(maxsize=50)
    
    print(f"[*] Đang khởi tạo đội ngũ {NUM_WORKERS} thợ Upload...")
    workers = []
    for i in range(NUM_WORKERS):
        workers.append(asyncio.create_task(upload_worker(f"THỢ {i+1}", creds, upload_queue, history_urls)))

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()
        
        print(f"\n[1] Đang mở link iCloud: {shared_link}")
        await page.goto(shared_link)
        
        print("[2] Đang chờ giao diện iCloud tải xong...")

        await page.wait_for_selector('.x-stream-photo-group-view', timeout=90000)
    
        print("Đang đợi Apple nạp dữ liệu...")
        await page.wait_for_timeout(5000) 
        
        print("[3] Đang mở chế độ xem toàn màn hình (Slideshow)...")
     
        first_image = page.locator('.x-stream-photo-group-view img').first

        await first_image.wait_for(state="attached", timeout=30000)

        await first_image.scroll_into_view_if_needed()

        await first_image.click(force=True)
        
        print("Đang chờ giao diện Slideshow tải...")
        await page.wait_for_selector('.slideshow-page', timeout=30000)
        await page.wait_for_timeout(2000)
        
        current_session_urls = set()
        count = 1
        if skip_count > 0:
            print(f"\n[TUA NHANH] Đang tua qua {skip_count} file đầu tiên với tốc độ cao...")
            for _ in range(skip_count):
                await page.keyboard.press("ArrowRight")
                await page.wait_for_timeout(300) 
            print("Đã tua xong! Chuyển về tốc độ tải bình thường...")
            count = skip_count + 1
       
        first_file_url = None
        previous_file_url = None
        stuck_count = 0
        
        print("\n=== BẮT ĐẦU QUY TRÌNH SONG SONG ===\n")
        while True:
            download_btn = page.locator('span.download.title.view.button, span[aria-label="Tải về"]')
            
            try:
                await page.mouse.move(500, 500)
                await page.wait_for_timeout(500)
                
                await download_btn.wait_for(state="attached", timeout=5000)
                
                async with page.expect_download() as download_info:
                    await download_btn.click(force=True)
                    
                download = await download_info.value
                file_name = download.suggested_filename
                # Chém đứt đuôi bảo mật dù Apple có dùng dấu ? hay dấu &
                file_url = download.url.split('?')[0].split('&')[0]
                # LOGIC NHẬN DIỆN "HẾT ALBUM" HOẶC "KẸT MẠNG"
                if first_file_url is None:
                    first_file_url = file_url 
                    previous_file_url = file_url
                else:
                    if file_url == previous_file_url:
                        stuck_count += 1
                        if stuck_count >= 3:
                            print("\n[HOÀN TẤT] Đã đến cuối album (Không thể lướt thêm)!")
                            await download.cancel()
                            break
                            
                        print(f"  -> UI chưa kịp chuyển ảnh (kẹt lần {stuck_count}), đang ép lướt tiếp...")
                        await download.cancel()
                        await page.keyboard.press("ArrowRight")
                        await page.wait_for_timeout(2000)
                        continue 
                        
                    elif file_url == first_file_url:
                         print("\n[HOÀN TẤT] Đã lướt 1 vòng quanh album và quay lại ảnh ban đầu!")
                         await download.cancel()
                         break
                         
                    else:
                        stuck_count = 0 # Thoát kẹt mạng, reset bộ đếm
                        previous_file_url = file_url
                if file_url in history_urls:
                    await download.cancel()
                else:
                    # Nếu file mới, tiến hành lưu và ném cho Worker
                    safe_file_name = f"{count}_{file_name}"
                    print(f"Tải về máy file thứ {count}: {file_name}")
                    
                    file_path = os.path.join(TEMP_DIR, safe_file_name)
                    await download.save_as(file_path)
                    
                    is_video = file_name.lower().endswith(('.mp4', '.mov', '.avi', '.m4v'))
                    folder_id = videos_folder_id if is_video else photos_folder_id
                    
                    await upload_queue.put((file_path, safe_file_name, folder_id, file_url))
                
            except Exception as e:
                pass # Bỏ qua nếu lướt qua lỗi hoặc giật mạng
            
            await page.keyboard.press("ArrowRight")
            await page.wait_for_timeout(1000)
            count += 1
                
        await browser.close()
        
        print("\nĐang chờ đội thợ dọn dẹp nốt số file còn lại trong kho...")
        await upload_queue.join()
        
        for _ in range(NUM_WORKERS):
            await upload_queue.put(None)
        await asyncio.gather(*workers)
        print("ĐÃ XONG TOÀN BỘ!")

# HÀM CHÍNH (MAIN)
def main():
    print("Đang khởi tạo kết nối Google Drive...")
    creds = get_drive_credentials()
    
    photos_folder_id = "14706UDubtg8pyzLv4ObMPjRRYPktzXnX"
    videos_folder_id = "1rNz6ODWwS_bcaPv04SfU-cPLIhWQziag"
    so_luong_tua = int(input("Nhập số file muốn tua nhanh (Nhập 0 nếu chạy từ đầu): "))
    asyncio.run(crawl_and_upload(LINK_CHIA_SE, creds, photos_folder_id, videos_folder_id, so_luong_tua))

if __name__ == "__main__":
    main()