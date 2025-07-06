from datetime import datetime
from collections import defaultdict
import time
import gspread
import re
from httplib2 import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
# from oauth2client.service_account import ServiceAccountCredentials
from gspread_formatting import set_row_heights # 🌟 Danh sách WooCommerce Stores & Google Sheets

class GoogleSheetHandler:
    def __init__(self, sheet_id):
        """Khởi tạo với ID Google Sheet"""
        self.sheet_id = sheet_id
        # self.client = gspread.Client(auth=None)  # Không cần xác thực, chỉ truy cập Google Sheet công khai
        self.client, self.service = self.authenticate_google_sheets()

    def authenticate_google_sheets(self):
        """Xác thực Google Sheets API"""
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        # creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Lay_link\linkDesign\credentials.json", scope)
        creds = service_account.Credentials.from_service_account_file(
            r"C:\Lay_link\linkDesign\credentials.json",
            scopes=scope
        )
        # client = gspread.authorize(creds)

        # Google API client để gọi batchUpdate (insert rows...)
        client = gspread.authorize(creds)
        service = build('sheets', 'v4', credentials=creds)
        return client,service


    def get_sheets(self):
        """Truy xuất Sheet1 và Sheet2 từ Google Sheets"""
        sheet = self.client.open_by_key(self.sheet_id)
        return sheet.worksheet("Sheet1"), sheet.worksheet("Sheet2")
    

    def add_rows_on_top(self, sheet_title, data_to_add):
        """
        Chèn số hàng = len(data_to_add) sau header (hàng 1) của sheet rồi ghi dữ liệu mới vào.

        :param sheet_title: Tên sheet (ví dụ "FF")
        :param data_to_add: List[List], dữ liệu mới cần thêm (dạng list các hàng)
        """
        ss = self.service.spreadsheets()
        # Lấy sheetId theo tên sheet
        spreadsheet = ss.get(spreadsheetId=self.sheet_id).execute()
        sheet_id = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == sheet_title:
                sheet_id = sheet['properties']['sheetId']
                break
        if sheet_id is None:
            raise Exception(f"Sheet '{sheet_title}' không tồn tại!")

        num_rows_to_insert = len(data_to_add)

        # Tạo request chèn hàng trống ngay dưới header (startIndex=1)
        requests = [{
            "insertDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": 1,
                    "endIndex": 1 + num_rows_to_insert
                },
                "inheritFromBefore": True
            }
        }]
        body = {'requests': requests}
        ss.batchUpdate(spreadsheetId=self.sheet_id, body=body).execute()

        # Ghi dữ liệu mới vào vùng vừa chèn
        sheet = self.client.open_by_key(self.sheet_id).worksheet(sheet_title)
        sheet.update(range_name = f"A2",values=  data_to_add)

        print(f"✅ Đã chèn {num_rows_to_insert} hàng mới lên đầu sheet '{sheet_title}' và cập nhật dữ liệu.")
    
    
    
    def copy_all_data_sheets(self, sheet_ids):
        """Lấy dữ liệu từ danh sách các Google Sheet"""
        all_data = []
        for sheet_id in sheet_ids:
            try:
                sheet = self.client.open_by_key(sheet_id)
                sheet_name = sheet.title  # Lấy tên của Google Sheet
                worksheet = sheet.worksheet("Sheet1")
                data = worksheet.get_all_values()  # Lấy tất cả dữ liệu của sheet
                if data :
                    if all_data == []:  
                        data[0].append("Store Name")  # Thêm tiêu đề cột mới vào hàng đầu tiên
                        for row in data[1:]:  # Bỏ qua hàng tiêu đề
                            row.append(sheet_name)
                    else : 
                        data.pop(0)
                        for row in data[0:]:  # Bỏ qua hàng tiêu đề
                            row.append(sheet_name)

                # Thêm tên sheet vào tất cả các hàng dữ liệu

                all_data.extend(data)
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu từ sheet {sheet_id}: {e}")

        return all_data  # Trả về tất cả dữ liệu đã thu thập
    
    def copy_all_data_sheets_date(self, sheet_ids,cutoff_date):
        """Lấy dữ liệu từ danh sách các Google Sheet"""
        all_data = []
        for sheet_id in sheet_ids:
            try:
                sheet = self.client.open_by_key(sheet_id)
                sheet_name = sheet.title  # Lấy tên của Google Sheet
                worksheet = sheet.worksheet("Sheet1")
                data = worksheet.get_all_values()  # Lấy tất cả dữ liệu của sheet
                if data :
                    headers = data[0]
                    try:
                        order_date_idx = headers.index("Order Date")
                    except ValueError:
                        print(f"'Order Date' không tồn tại trong sheet {sheet_name}")
                        continue
                    if all_data == []:  
                        headers.append("Store Name")
                        all_data.append(headers)
                    for row in data[1:]:
                        if len(row) <= order_date_idx:
                            continue  # Bỏ qua nếu hàng không đủ cột

                        order_date_str = row[order_date_idx]
                        try:
                            order_date = datetime.strptime(order_date_str, "%Y-%m-%dT%H:%M:%S")
                            if  cutoff_date < order_date:
                                row.append(sheet_name)
                                all_data.append(row)
                        except ValueError:
                            continue  # Bỏ qua nếu không đúng định dạng
                # Thêm tên sheet vào tất cả các hàng dữ liệu

                # all_data.extend(data)
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu từ sheet {sheet_id}: {e}")

        return all_data  # Trả về tất cả dữ liệu đã thu thập
    
    def link_design_hog(seft,order_id,data_design_hog):
        try :
            for row in data_design_hog :
                if len(row) > 4 and row[4] == order_id :
                    if len(row) > 20 : return row[21]
                    break
        except Exception as e:
            print(f"❌ Lỗi khi tìm Link Design cho Order ID {order_id}: {e}")

        return ""  # Không tìm thấy, trả về chuỗi rỗng
        
    
    def link_design_mf(self,order_id,shoes_data,cn_data) :
        """
        Lấy link thiết kế từ Google Sheet với ID 1Y_EnKwWThJaxLaLQyAWGojCjcahJscZPCve5qHbwGIs.
        - Nếu order_id tồn tại trong sheet 'Shoes' (cột D), lấy giá trị từ cột Q.
        - Nếu order_id tồn tại trong sheet 'CN' (cột C), lấy giá trị từ cột V.
        - Nếu không tìm thấy, trả về "".

        :param order_id: Mã đơn hàng cần tìm link thiết kế.
        :return: Link thiết kế nếu tìm thấy, nếu không trả về "".
        """
        try:
            # Mở Google Sheet
            # sheet = self.client.open_by_key("1Y_EnKwWThJaxLaLQyAWGojCjcahJscZPCve5qHbwGIs")

            # # 🔹 Kiểm tra trong Sheet 'Shoes'
            # shoes_sheet = sheet.worksheet("Shoes")
            # shoes_data = shoes_sheet.get_all_values()
            
            # Duyệt cột D để tìm order_id
            for row in shoes_data:
                if len(row) > 3 and row[3] == order_id:  # Cột D (index 3)
                    if len(row) > 16:  # Cột Q (index 16)
                        return row[16]  # Trả về link từ cột Q
                    break


            # 🔹 Nếu không tìm thấy, kiểm tra trong Sheet 'CN'
            # cn_sheet = sheet.worksheet("CN")
            # cn_data = cn_sheet.get_all_values()
            
            # Duyệt cột C để tìm order_id
            for row in cn_data:
                if len(row) > 2 and row[2] == order_id:  # Cột C (index 2)
                    if len(row) > 21:  # Cột V (index 21)
                        return row[21]  # Trả về link từ cột V
                    break

        except Exception as e:
            print(f"❌ Lỗi khi tìm Link Design cho Order ID {order_id}: {e}")

        return ""  # Không tìm thấy, trả về chuỗi rỗng

    def update_cell(self, row, col, value):
        """Cập nhật giá trị vào ô (row, col) trong Sheet2"""
        try:
            sheet2 = self.get_sheets()[1]  # Lấy Sheet2
            sheet2.update_cell(row, col, value)  # Cập nhật giá trị
            print(f"✅ Đã cập nhật ô ({row}, {col}) với giá trị: {value}")
        except Exception as e:
            print(f"❌ Lỗi khi cập nhật ô ({row}, {col}): {e}")

    def check_link_template_hog(self,sheet_data,sku) :
        try:
            for row in sheet_data:
                if row[0] == sku and sku and row[0] :
                    return row[1]                
            return ""

        except Exception as e:
            print(f"❌ Lỗi khi tìm Link template cho SKU {sku} : {e}")

        return ""  # Không tìm thấy, trả về chuỗi rỗng

    def check_link_template_mf(self,sheet_data,sku) :
        try:
            for row in sheet_data:
                if row[0] == sku and sku and row[0] :
                    return row[1]                
            return ""

        except Exception as e:
            print(f"❌ Lỗi khi tìm Link template cho SKU {sku} : {e}")

        return ""  # Không tìm thấy, trả về chuỗi rỗng
    
    def generate_sheet3(self):    
        try:
            sheet1, _ = self.get_sheets()
            data = sheet1.get_all_values()
            time.sleep(60)
            if len(data) <= 1:
                print("⚠️ Sheet1 không có đủ dữ liệu.")
                return

            headers = data[0]
            idx_order_date = headers.index("Order Date")
            idx_store = headers.index("Store Name")
            idx_order_id = headers.index("Order ID")
            idx_order_status = headers.index("Order Status")
            idx_link_url = headers.index("Link ULR")
            idx_quantity = headers.index("Quantity")
            idx_unit_cost = headers.index("Unit Cost")
            idx_total_cost = headers.index("Total cost")
            idx_shipping_total = headers.index("Shipping Total")
            idx_order_total = headers.index("Order Total")

            # ✅ Group by date -> store
            grouped = defaultdict(lambda: defaultdict(list))

            for row in data[1:]:
                if row[idx_order_status].lower() == "failed":
                    continue
                raw_date = row[idx_order_date].split("T")[0].split(" ")[0]
                store = row[idx_store]
                grouped[raw_date][store].append([
                    row[idx_order_id],
                    row[idx_order_status],
                    row[idx_link_url],
                    row[idx_quantity],
                    row[idx_unit_cost],
                    row[idx_total_cost],
                    row[idx_shipping_total],
                    row[idx_order_total],
                ])
            # ✅ Chuẩn bị dữ liệu Sheet3
            output = [["Order Date", "Store Name", "Order ID", "Order Status", "Link ULR", "Quantity", "Unit Cost", "Total Cost", "Shipping Total", "Order Total"]]

            for date in sorted(grouped.keys(), reverse=True):
                stores = grouped[date]
                date_str = f"ngày {datetime.strptime(date, '%Y-%m-%d').strftime('%d/%m')}"
                output.append([date_str])  # dòng hiển thị ngày
                for store in stores:
                    output.append(["", store])  # dòng hiển thị store
                    total = 0
                    for order in stores[store]:
                        try:
                            total += float(order[7])
                        except:
                            pass
                        output.append(["", "", *order])
                    output.append(["", "", "", "", "", "", "", "", "", f"Tổng tiền {store} trong ngày", f"{total:.2f}"])
                output.append([])  # dòng trống phân cách ngày

            # ✅ Ghi vào Sheet3
            sheet = self.client.open_by_key(self.sheet_id)
            if "Sheet3" in [ws.title for ws in sheet.worksheets()]:
                sheet3 = sheet.worksheet("Sheet3")
                sheet3.clear()
            else:
                sheet3 = sheet.add_worksheet(title="Sheet3", rows="10000", cols="15")

            sheet3.update(range_name = "A1",values = output)
            print("✅ Đã tạo Sheet3 đúng định dạng thiết kế chuẩn như ảnh bạn gửi.")

        except Exception as e:
            print(f"❌ Lỗi khi tạo Sheet3: {e}")

    def copy_all_data_sheet2(self, sheet_ids):
        """Lấy dữ liệu từ Sheet2 của danh sách các Google Sheet"""
        all_data = []
        for sheet_id in sheet_ids:
            try:
                sheet = self.client.open_by_key(sheet_id)
                sheet_name = sheet.title
                worksheet = sheet.worksheet("Sheet2")
                data = worksheet.get_all_values()
                if data:
                    if not all_data:
                        data[0].append("Store Name")
                        for row in data[1:]: row.append(sheet_name)
                    else:
                        data.pop(0)
                        for row in data: row.append(sheet_name)
                all_data.extend(data)
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu từ sheet2 {sheet_id}: {e}")
        return all_data
    
    def copy_all_data_sheet2_date(self, sheet_ids,cutoff_date):
        """Lấy dữ liệu từ Sheet2 của danh sách các Google Sheet"""
        all_data = []
        for sheet_id in sheet_ids:
            try:
                sheet = self.client.open_by_key(sheet_id)
                sheet_name = sheet.title
                worksheet = sheet.worksheet("Sheet2")
                data = worksheet.get_all_values()
                if data:
                    headers = data[0]
                    try:
                        order_date_idx = headers.index("Order Date")
                    except ValueError:
                        print(f"'Order Date' không tồn tại trong sheet {sheet_name} (Sheet2)")
                        continue

                    if not all_data:
                        headers.append("Store Name")
                        all_data.append(headers)

                    for row in data[1:]:
                        if len(row) <= order_date_idx:
                            continue

                        order_date_str = row[order_date_idx]
                        try:
                            order_date = datetime.strptime(order_date_str, "%Y-%m-%dT%H:%M:%S")
                            if cutoff_date < order_date:
                                row.append(sheet_name)
                                all_data.append(row)
                        except ValueError:
                            continue  # Bỏ qua nếu không đúng định dạng
                # all_data.extend(data)
            except Exception as e:
                print(f"Lỗi khi lấy dữ liệu từ sheet2 {sheet_id}: {e}")
        return all_data
    
    def generate_sheet4(self,sheet2_data):
        """Tạo Sheet4: copy cột Order Date, Order ID, Custom Number, Store Name và cập nhật Status Checking, Xưởng"""
        time.sleep(60)
        try:
            sheet1, _ = self.get_sheets()
            data1 = sheet1.get_all_values()
            if len(data1) <= 1:
                print("⚠️ Sheet1 không có đủ dữ liệu để tạo Sheet4.")
                return
            headers1 = data1[0]
            idx_date = headers1.index("Order Date")
            idx_status = headers1.index("Order Status")
            idx_order = headers1.index("Order ID")
            idx_custom = headers1.index("Number Checking")
            idx_store = headers1.index("Store Name")
            print('11111111111')

            # Lấy dữ liệu Sheet2 từ cùng các sheet nguồn đã dùng copy
            # Giả sử self.source_sheet_ids tồn tại
            time.sleep(60)
            # Tạo map order_id -> Status (cột F trước khi append Store Name)
            status_map = {row[idx_order]: row[5] for row in sheet2_data[1:] if len(row) > 5}

            # Lấy dữ liệu thiết kế MF, HOG, WEB
            mf_sheet = self.client.open_by_key("1Y_EnKwWThJaxLaLQyAWGojCjcahJscZPCve5qHbwGIs").worksheet("CN")
            shoes_sheet = self.client.open_by_key("1Y_EnKwWThJaxLaLQyAWGojCjcahJscZPCve5qHbwGIs").worksheet("Shoes")
            hog_sheet = self.client.open_by_key("1jDZbTZzUG-_Sw3NXgKMjRa5YD9V3PjMkLlx78-w688Y").worksheet("3D(BY SELLER)")
            hog_sheet_2d = self.client.open_by_key("1jDZbTZzUG-_Sw3NXgKMjRa5YD9V3PjMkLlx78-w688Y").worksheet("IN 2D (BY SELLER)")
            tp_sheet = self.client.open_by_key("13agKuW62InJ_Sdj0qA5SmiHJYjiPFqguUllLjr3CzM4").worksheet("ORDER JERSEY")
            web_sheet = self.client.open_by_key("1mCdTlRUw2OlNLBipZWycfP6CDhJ29DZBNF7zv2snoB4").worksheet("WEB")
            web_sheet_c_nhung = self.client.open_by_key("1rzAqanj3oekf-b_jAyAQL9dXZ2b374aGLfz1-6mPomw").worksheet("FF")
            cn_ids = {row[2]: True for row in mf_sheet.get_all_values()[1:] if len(row) > 2}
            shoes_ids = {row[3]: True for row in shoes_sheet.get_all_values()[1:] if len(row) > 3}
            hog_ids = {row[4]: True for row in hog_sheet.get_all_values()[1:] if len(row) > 4}
            hog_id_2d = {row[4]: True for row in hog_sheet_2d.get_all_values()[1:] if len(row) > 4}
            tp_id = {row[10]: True for row in tp_sheet.get_all_values()[1:] if len(row) > 4}
            web_rows = {row[idx_order]: row[0] for row in web_sheet.get_all_values()[1:] if len(row) > idx_order}
            web_row_c_nhung = {row[idx_order]: row[38] for row in web_sheet_c_nhung.get_all_values()[1:] if len(row) > idx_order}
            
            print('2222222222222222')
            time.sleep(60)
            # Chuẩn bị dữ liệu cho Sheet4
            output = [["Order Date", "Order ID","Status Đơn Hàng", "Number Checking", "Store Name", "Status Checking", "Xưởng"]]
            for row in data1[1:]:
                oid = row[idx_order]
                status = status_map.get(oid, "")
                # Xác định xưởng
                xuong = ""
                if oid in shoes_ids or oid in cn_ids or oid in hog_ids or oid in hog_id_2d or oid in tp_id:
                    if oid in shoes_ids or oid in cn_ids:
                        xuong = "MF"
                    if oid in hog_ids or oid in hog_id_2d:
                        xuong = xuong + "- HOG" if xuong else "HOG"
                    if oid in tp_id:
                        xuong = xuong +  "- TP" if xuong else "TP"
                else :
                    if oid in web_rows :
                       xuong = xuong + "-" + web_rows[oid] if xuong else web_rows[oid]
                    elif oid in web_row_c_nhung :
                       xuong = xuong + "-" + web_row_c_nhung[oid] if xuong else web_row_c_nhung[oid]
                # if oid in shoes_ids or oid in cn_ids:
                #     xuong = "MF"
                # elif oid in hog_ids or oid in hog_id_2d:
                #     xuong = xuong + "- HOG" if xuong else "HOG"
                # elif oid in tp_id:
                #     xuong = xuong +  "- TP" if xuong else "TP"
                # elif oid in web_rows:
                #     xuong = xuong + "-" + web_rows[oid] if xuong else web_rows[oid]
                # else:
                #     xuong = ""
                output.append([row[idx_date], oid,row[idx_status], row[idx_custom], row[idx_store], status, xuong])

            sheet = self.client.open_by_key(self.sheet_id)
            print('333333333333')
            time.sleep(60)
            if "Tracking Auto" in [ws.title for ws in sheet.worksheets()]:
                sheet4 = sheet.worksheet("Tracking Auto")
                sheet4.clear()
            else:
                sheet4 = sheet.add_worksheet(title="Tracking Auto", rows=str(len(output)+10), cols="6")
            sheet4.update(range_name = "A1:G1",values =  [output[0]])
            sheet4.update(range_name = "A2:G{}".format(len(output)),values = output[1:])
            print("✅ Đã tạo Sheet4 với cột Status Checking và Xưởng.")
        except Exception as e:
            print(f"❌ Lỗi khi tạo Sheet4: {e}")


    def extract_slug(self,url):
        remove_words = {"luxinshoes", "davidress", "onesimpler", "xanawood", "lovasuit", "luxinhoes","clomic"}
        # Bước 1: Trích xuất phần slug từ URL
        match = re.search(r'/product/([\w-]+?)(?:-\d+)?/$', url)  
        if not match:
            return None
        slug = match.group(1)

        # Bước 2: Loại bỏ các từ không mong muốn (bao gồm cả từ nằm trong chuỗi khác)
        for word in remove_words:
            slug = re.sub(rf'\b{word}\b', '', slug, flags=re.IGNORECASE)  # Xóa từ nguyên vẹn
            slug = re.sub(rf'{word}', '', slug, flags=re.IGNORECASE)  # Xóa từ nếu nằm trong chuỗi khác

        # Bước 3: Chuẩn hóa chuỗi sau khi loại bỏ từ không mong muốn
        slug = re.sub(r'-+', '-', slug).strip('-')  # Xóa dấu `-` dư thừa

        return slug if slug else None
    def update_link_design(self,sheet):
        """
        Kiểm tra nếu có hàng bên trên có cột K và cột L giống bất kỳ hàng bên dưới, 
        thì sẽ gán giá trị cột N của hàng bên dưới lên hàng bên trên.
        
        :param sheet: Google Sheet cần kiểm tra.
        """
        try:
            # Lấy toàn bộ dữ liệu của sheet
            data = sheet.get_all_values()

            if len(data) <= 1:
                print("⚠️ Sheet không có đủ dữ liệu để kiểm tra.")
                return

            # Xác định chỉ số cột
            headers = data[0]
            try:
                col_K_idx = headers.index("Link ULR")  # Cột K
                col_N_idx = headers.index("Check Design")  # Cột K

                col_L_idx = headers.index("Link Template Hog")  # Cột L
                col_M_idx = headers.index("Link Template MF")  # Cột M
                
                col_O_idx = headers.index("Link Design Hog")  # Cột N
                col_P_idx = headers.index("Link Design MF")  # Cột N

                # col_M_idx = headers.index("Check Design") #cột M
            except ValueError:
                print("❌ Không tìm thấy cột K, L hoặc N trong sheet.")
                return

            # Duyệt từ trên xuống để kiểm tra
            updates_mf = []
            updates_hog = []
            for i in range(1, len(data) - 1):  # Bỏ qua dòng tiêu đề
                K_value = self.extract_slug(data[i][col_K_idx]) 
                M_value = data[i][col_M_idx]
                if not data[i][col_N_idx] :        
                    for j in range(i + 1, len(data)):  # Kiểm tra các hàng bên dưới
                        if self.extract_slug(data[j][col_K_idx])  == K_value and data[j][col_M_idx] == M_value:
                            if data[j][col_P_idx]:  # Nếu cột N của hàng bên dưới có giá trị
                                updates_mf.append((i + 1, col_P_idx + 1, data[j][col_P_idx]))  # Lưu cập nhật
                                break

            for i in range(1, len(data) - 1):  # Bỏ qua dòng tiêu đề
                K_value = self.extract_slug(data[i][col_K_idx]) 
                L_value = data[i][col_L_idx]

                if not data[i][col_N_idx] :        
                    for j in range(i + 1, len(data)):  # Kiểm tra các hàng bên dưới
                        if self.extract_slug(data[j][col_K_idx])  == K_value and data[j][col_L_idx] == L_value:
                            if data[j][col_O_idx]:  # Nếu cột N của hàng bên dưới có giá trị
                                updates_hog.append((i + 1, col_O_idx + 1, data[j][col_O_idx]))  # Lưu cập nhật
                                break

            # Thực hiện cập nhật vào Google Sheet
            if updates_mf:
                batch_update_requests_mf = [
                    {"range": f"P{row}", "values": [[value]]} for row, _, value in updates_mf
                ]
                sheet.batch_update(batch_update_requests_mf)
                print(f"✅ Đã cập nhật {len(updates_mf)} hàng trong cột N.")

            if updates_hog:
                batch_update_requests_hog = [
                    {"range": f"O{row}", "values": [[value]]} for row, _, value in updates_hog
                ]
                sheet.batch_update(batch_update_requests_hog)
                print(f"✅ Đã cập nhật {len(updates_hog)} hàng trong cột N.")

            else:
                print("⚠️ Không có hàng nào cần cập nhật.")

        except Exception as e:
            print(f"❌ Lỗi khi kiểm tra và cập nhật cột N: {e}")

    
    def update_sheet2(self):
        sheet1, sheet2 = self.get_sheets()
        data1 = sheet1.get_all_values()
        data2 = sheet2.get_all_values()
        
        if len(data1) <= 1:
            print("Sheet nguồn không có dữ liệu.")
            return
        headers = data1[0]
        print('headers',headers)
        try :
            order_date_idx = headers.index("Order Date")
            order_id_idx = headers.index("Order ID")
            note_idx = headers.index("Note")
            custom_name_idx = headers.index("Custom Name")
            custom_number_idx = headers.index("Custom Number")
            sku_idx = headers.index("SKU")
            store_idx = headers.index("Store Name")
            type_idx = headers.index("Type")
            link_image_idx = headers.index("Link image")
            link_url_idx = headers.index("Link ULR")
            order_status_idx = headers.index("Order Status")
        except ValueError:
            print("❌ Không tìm thấy một hoặc nhiều cột cần thiết.")
            return
        print('-------------------')
        design_sheet_mf = self.client.open_by_key("1Y_EnKwWThJaxLaLQyAWGojCjcahJscZPCve5qHbwGIs")
        shoes_sheet = design_sheet_mf.worksheet("Shoes")
        shoes_data = shoes_sheet.get_all_values()
        cn_sheet = design_sheet_mf.worksheet("CN")
        cn_data = cn_sheet.get_all_values()

        template_sheet_mf = self.client.open_by_key("1Uw8FQVI2ef4ANZX8pEPEO19oDSrJBzpnRIIGcV7kmUM")
        sheet_template = template_sheet_mf.worksheet("Sheet1")
        data_template_mf = sheet_template.get_all_values()

        design_sheet_hog = self.client.open_by_key("1jDZbTZzUG-_Sw3NXgKMjRa5YD9V3PjMkLlx78-w688Y")
        shee1_sheet_hog = design_sheet_hog.worksheet("3D(BY SELLER)")
        sheet_t6_sheet_hog = design_sheet_hog.worksheet("THÁNG 6-BY SELLER")
        data_shee1 = shee1_sheet_hog.get_all_values()
        data_sheet6 = sheet_t6_sheet_hog.get_all_values()
        data_design_hog = data_shee1 + data_sheet6[1:]

        template_sheet_hog = self.client.open_by_key("1ctlPBJ6NvS2z59lJqHeNYyIvk3k1YSISO7CdIas0xjA")
        sheet1_template_hog = template_sheet_hog.worksheet("Sheet1")
        data_template_hog = sheet1_template_hog.get_all_values()
        # ✅ Lưu vào dictionary để tra cứu nhanh hơn
        if len(data2) <=1 :
                new_data = []
                new_data.append(["Order Date","Order ID","Note","Custom Name","Custom Number","SKU","Store Name","Type","Link image","image","Link ULR"])
                for i in range(1,len(data1)) :
                    row = data1[i]
                    order_status = row[order_status_idx]
                    order_date = row[order_date_idx]
                    order_id = row[order_id_idx]
                    note = row[note_idx]
                    custom_name = row[custom_name_idx]
                    custom_number = row[custom_number_idx]
                    sku = row[sku_idx]
                    store = row[store_idx]
                    type = row[type_idx]
                    link_image = row[link_image_idx]
                    link_url = row[link_url_idx]
                    if order_status != "failed" and sku != "AODAU":
                        new_data.append([order_date,order_id,note,custom_name,custom_number,sku,store,type ,link_image,"",link_url])
                sheet2.update(range_name = "A1",values = new_data)
                self.apply_formula_to_cells(sheet2,"J")
                return
        else : 
            dest_order_map_mf = {}
            for j in range(1,len(data2)) :
                dest_order_id = data2[j][1]
                dest_check_design = data2[j][13]
                dest_link_design_mf = data2[j][14]
                if dest_order_id:
                    dest_order_map_mf[dest_order_id] = {"row" : j +1 ,"Link Design" : dest_link_design_mf,"Check Design":dest_check_design }
            # dest_link_design_hog = data2[j][13]

            new_rows = []
            updated_rows = []
            for i in range(1, len(data1)):
                row = data1[i]
                order_status = row[order_status_idx]
                order_date = row[order_date_idx]
                order_id = row[order_id_idx]
                note = row[note_idx]
                custom_name = row[custom_name_idx]
                custom_number = row[custom_number_idx]
                sku = row[sku_idx]
                store = row[store_idx]
                type = row[type_idx]
                link_image = row[link_image_idx]
                link_url = row[link_url_idx]
                if order_id in dest_order_map_mf :
                    dest_row_index = dest_order_map_mf[order_id]["row"]
                    dest_check_design = dest_order_map_mf[order_id]["Check Design"]

                    # dest_link_design_mf = dest_order_map_mf[order_id]["Link Design MF"]
                    # dest_link_design_hog = dest_order_map_mf[order_id]["Link Design Hog"]

                    link_design_mf = self.link_design_mf(order_id,shoes_data,cn_data)
                    link_design_hog = self.link_design_hog(order_id,data_design_hog)
                    if (link_design_mf or link_design_hog) and not dest_check_design: 
                        updated_rows.append({"row" : dest_row_index,"value_hog" : link_design_hog,"value_mf" : link_design_mf })
                else : 
                    if order_status != "failed" and sku != "AODAU":
                        link_design_hog = self.link_design_mf(order_id,shoes_data,cn_data)
                        link_design_mf = self.link_design_mf(order_id,shoes_data,cn_data)
                        check_design = "da ff" if link_design_hog or link_design_mf else '' 
                        new_rows.append([order_date,order_id,note,custom_name,custom_number,sku,store,type ,link_image,"",link_url,"","",check_design,link_design_hog,link_design_mf])
            update_requests = []
            for update in updated_rows:
                update_requests.append({
                    "range": f"N{update['row']}:P{update['row']}",  # ✅ Từ cột N đến P (3 cột liền nhau)
                    "values": [["da ff", update["value_hog"], update["value_mf"]]]
                })

            sheet2.batch_update(update_requests)
            if new_rows :
                sheet2.append_rows(new_rows)
                print(f"✅ Đã thêm {len(new_rows)} đơn hàng mới vào Sheet2.")
            self.sort_sheet(sheet2,  0)
            self.apply_formula_to_cells(sheet2,"J")
        sheet1Update, sheet2Update = self.get_sheets()
        data2update = sheet2Update.get_all_values()
        update_template_hog = []
        update_template_mf = []
        for i in range(1,len(data2update)) : 
            if '-' in data2update[i][5]:
                parts = data2update[i][5].split('-', 1)  # chỉ tách 1 lần
                sku_hog = parts[0].strip()
                sku_mf = parts[1].strip()
            else :
                sku_hog = data2update[i][5] 
                sku_mf = data2update[i][5] 
            if  not data2update[i][11] : 
                link_template_hog = self.check_link_template_hog(data_template_hog,sku_hog)
                if(link_template_hog) : 
                    update_template_hog.append({
                        "range": f"L{i+1}",  # Cột 12 (L)
                        "values": [[link_template_hog]]
                    })

                # if not data2update[i][13] : pass
            
            if  not data2update[i][12] : 
                link_template_mf = self.check_link_template_mf(data_template_mf,sku_mf)
                if(link_template_mf) : 
                    update_template_mf.append({
                        "range": f"M{i+1}",  # Cột 12 (M)
                        "values": [[link_template_mf]]
                    })

                # if not data2update[i][13] : pass
        if update_template_hog : 
            sheet2Update.batch_update(update_template_hog)
        if update_template_mf : 
            sheet2Update.batch_update(update_template_mf)
        self.update_link_design(sheet2Update)

    def sort_sheet(self, sheet, sort_col):
        """
        Sắp xếp Google Sheet theo cột chứa ngày tháng (format: YYYY-MM-DD HH:MM:SS).
        
        :param sheet: Google Sheet cần sắp xếp.
        :param sort_col: Chỉ mục của cột cần sắp xếp (A = 0, B = 1, ...)
        """
        data = sheet.get_all_values()
        headers = data[0]

        # ✅ Chuyển đổi giá trị ngày tháng thành `datetime`
        def parse_date(value):
            try:
        # Nếu dùng khoảng trắng thay vì 'T'
                if " " in value:
                    value = value.replace(" ", "T")
                # Nếu giờ < 10 mà không có 0, thì chuẩn hóa lại giờ phút giây thành 2 chữ số
                date_part, time_part = value.split("T")
                time_parts = time_part.split(":")
                if len(time_parts[0]) == 1:
                    time_parts[0] = time_parts[0].zfill(2)  # Thêm số 0 trước giờ nếu cần
                if len(time_parts[1]) == 1:
                    time_parts[1] = time_parts[1].zfill(2)
                if len(time_parts[2]) == 1:
                    time_parts[2] = time_parts[2].zfill(2)
                value = f"{date_part}T{':'.join(time_parts)}"
                return datetime.fromisoformat(value)
            except Exception:
                return datetime.min

        # ✅ Sắp xếp dữ liệu theo ngày (mới nhất -> cũ nhất)
        sorted_data = sorted(data[1:], key=lambda x: parse_date(x[sort_col]), reverse=True)

        # ✅ Xóa dữ liệu cũ và cập nhật dữ liệu mới
        sheet.clear()
        batch_size = 500
        sheet.clear()
        sheet.update(range_name = "A1",values = [headers])
        


        for i in range(0, len(sorted_data), batch_size):
            batch = sorted_data[i:i+batch_size]
            start_row = i + 2  # +2 vì header ở hàng 1
            end_row = start_row + len(batch) - 1
            num_cols = max(len(row) for row in batch)
            end_col_letter = gspread.utils.rowcol_to_a1(1, num_cols).replace("1", "")
            range_str = f"A{start_row}:{end_col_letter}{end_row}"  # Z là cột tùy chỉnh cho đủ rộng
            sheet.update(range_name = range_str,values = batch)
        # sheet.append_rows([headers] + sorted_data)
        
        print(f"✅ Đã sắp xếp Sheet theo cột {headers[sort_col]} (Ngày mới nhất -> cũ nhất).")
    
    def generate_sheet_ff(self):
        """
        Tạo / cập nhật sheet "FF":
        - Kiểm tra Order ID đã tồn tại chưa, nếu có thì bỏ qua.
        - Chỉ copy những hàng Order Status != 'failed'.
        - Loại trừ 2 cột: 'factory' và 'Number Checking'.
        - Thêm 6 cột mới (tất cả để trống): 
        ['Classification', 'Link Template', 'Link Design',
        'Xưởng', 'Has Design Link', 'Has FF']
        """
        # 1. Chuẩn bị dữ liệu
        sheet1, _ = self.get_sheets()
        data = sheet1.get_all_values()
        if len(data) <= 1:
            print("⚠️ Sheet1 không có dữ liệu hợp lệ để tạo FF.")
            return

        headers = data[0]
        rows = data[1:]

        # Xác định các chỉ số cột quan trọng
        idx_order = headers.index("Order ID")
        idx_status = headers.index("Order Status")

        # Xác định các cột cần loại trừ (factory, Number Checking)
        exclude_cols = {"factory", "Number Checking"}
        cols_to_copy = [h for h in headers if h not in exclude_cols]

        # Xác định các cột cần loại trừ trong sheet Design (factory, Number Checking)
        exclude_cols_ds = {"factory", "Number Checking","Order Status","Pay URL","Product Size","Product ID","Lineitem Sku","Shipping Address 1","Shipping Address 2","Shipping City","Shipping Zipcode","Shipping State","Shipping Country","BillingPhone","ShippingPhone","Email","Quantity","Shipping Total","Order Total","Unit Cost","Total cost",}
        cols_to_copy_ds = [h for h in headers if h not in exclude_cols_ds]
        # 2. Lấy sheet FF (nếu chưa có thì tạo mới)
        ss = self.client.open_by_key(self.sheet_id)
        titles = [ws.title for ws in ss.worksheets()]
        if "FF" in titles:
            ff = ss.worksheet("FF")
            existing = {r[1] for r in ff.get_all_values()[1:]}  # Order ID ở cột B
        else:
            ff = ss.add_worksheet(title="FF", rows="10000", cols=str(len(cols_to_copy) + 6))
            existing = set()

        if "DS" in titles:
            ds = ss.worksheet("DS")
            existing_ds = {r[1] for r in ds.get_all_values()[1:]}  # Order ID ở cột B
        else:
            ds = ss.add_worksheet(title="DS", rows="10000", cols=str(len(cols_to_copy_ds) + 4))
            existing_ds = set()

        # 3. Xây header cho FF: 
        #    tất cả cols_to_copy + 6 cột mới
        # new_headers = cols_to_copy + [
        #     "Classification", "Link Template", "Link Design",
        #     "Xưởng", "Has Design Link", "Has FF"
        # ]
        # ff.append_row(new_headers)

        # 4. Duyệt mỗi dòng, lọc và append
        to_append = []
        to_append_ds = []
        for r in rows:
            oid = r[idx_order]
            status = r[idx_status].lower()
            if oid in existing or status == "failed" or status == "checkout-draft":
                continue
            # giữ lại chỉ các cột trong cols_to_copy
            base = [r[headers.index(h)] for h in cols_to_copy]
            # thêm 6 trường mới, khởi tạo "" 
            base += [""] * 6
            to_append.append(base)

        for r in rows:
            oid = r[idx_order]
            status = r[idx_status].lower()
            if oid in existing_ds or status == "failed" or status == "checkout-draft":
                continue
            # giữ lại chỉ các cột trong cols_to_copy
            base_ds = [r[headers.index(h)] for h in cols_to_copy_ds]
            # thêm 6 trường mới, khởi tạo "" 
            base_ds += [""] * 5
            to_append_ds.append(base_ds)

        if to_append:
            self.add_rows_on_top("FF",to_append)
            print(f"✅ Đã thêm {len(to_append)} hàng mới vào sheet FF.")
        else:
            print("⚠️ Không có hàng mới nào cần thêm vào FF.")

        time.sleep(60)
        if to_append_ds:
            self.add_rows_on_top("DS",to_append_ds)
            print(f"✅ Đã thêm {len(to_append_ds)} hàng mới vào sheet DS.")
        else:
            print("⚠️ Không có hàng mới nào cần thêm vào DS.")
        time.sleep(60)
        print("✅ bắt đầu lưu công thức vào cột image" )
        self.apply_formula_to_cells(ff,"AC")
        time.sleep(60)
        set_row_heights(ff, [('1:10000', 100)])  # Đặt chiều cao tất cả các hàng từ 1 đến 1000 là 100px
        set_row_heights(ds, [('1:10000', 100)])  # Đặt chiều cao tất cả các hàng từ 1 đến 1000 là 100px
        print("✅ Đã đặt chiều cao tất cả các hàng thành 100px")

    def import_design(self):
        """
        Kiểm tra tất cả các hàng trong sheet DS.
        Với mỗi hàng ở vị trí i (bắt đầu từ hàng 2) so sánh với cùng vị trí i của sheet FF.
        Nếu ở FF cột 'Has Design Link' hoặc 'Has FF' đã là 'done', bỏ qua.
        Nếu chưa, nhập giá trị 'Link Design' của DS vào 'Link Design' của FF
        và cột 'Has Design Link' của FF điền 'done' và điền của giá trị note vào.
        """
        try:
            ss = self.client.open_by_key(self.sheet_id)
            # Lấy dữ liệu từ sheet DS và FF
            if "DS" not in [ws.title for ws in ss.worksheets()]:
                print("⚠️ Không tìm thấy sheet 'DS'.")
                return
            if "FF" not in [ws.title for ws in ss.worksheets()]:
                print("⚠️ Không tìm thấy sheet 'FF'.")
                return

            sheet_ds = ss.worksheet("DS")
            sheet_ff = ss.worksheet("FF")
            data_ds = sheet_ds.get_all_values()
            data_ff = sheet_ff.get_all_values()
            if len(data_ds) <= 1 or len(data_ff) <= 1:
                print("⚠️ Dữ liệu trong DS hoặc FF không đủ để xử lý.")
                return

            # Xác định chỉ số cột trong DS
            headers_ds = data_ds[0]
            try:
                idx_link_design_ds = headers_ds.index("Link Design")
                idx_not_ds = headers_ds.index("Note")
            except ValueError:
                print("❌ Không tìm thấy cột 'Order ID' hoặc 'Link Design' trong DS.")
                return

            # Xác định chỉ số cột trong FF
            headers_ff = data_ff[0]
            try:
                idx_link_design_ff = headers_ff.index("Link Design")
                idx_has_design_link_ff = headers_ff.index("Has Design Link")
                idx_has_ff_ff = headers_ff.index("Has FF")
                idx_not_ff = headers_ff.index("Note")
            except ValueError:
                print("❌ Không tìm thấy một hoặc nhiều cột cần thiết trong FF ('Order ID', 'Link Design', 'Has Design Link', 'Has FF').")
                return

            # Lấy số lượng hàng (bao gồm header) của hai sheet
            num_rows_ds = len(data_ds)
            num_rows_ff = len(data_ff)

            # Ta sẽ duyệt i từ 1 đến min(num_rows_ds, num_rows_ff) - 1,
            # tương ứng lần lượt là các hàng dữ liệu (bỏ qua header ở i=0)
            max_i = min(num_rows_ds, num_rows_ff)

            update_requests = []
            for i in range(1, max_i):
                row_ds = data_ds[i]
                row_ff = data_ff[i]

                # Lấy giá trị Link Design và Note từ DS (nếu có)
                link_design_ds = row_ds[idx_link_design_ds].strip() if len(row_ds) > idx_link_design_ds else ""
                note_ds = row_ds[idx_not_ds].strip() if len(row_ds) > idx_not_ds else ""

                # Lấy trạng thái Has Design Link và Has FF từ FF (nếu có)
                has_design_link = row_ff[idx_has_design_link_ff].strip() if len(row_ff) > idx_has_design_link_ff else ""
                has_ff = row_ff[idx_has_ff_ff].strip() if len(row_ff) > idx_has_ff_ff else ""

                # Nếu DS không có Link Design hoặc FF đã "done" thì bỏ qua
                if not link_design_ds:
                    continue
                if has_design_link.lower() == "done" or has_ff.lower() == "done":
                    continue

                # Tính số dòng thực tế trong Google Sheets (1-based), header là dòng 1 => data hàng thứ i tương ứng row number i+1
                row_number_ff = i + 1

                # Chuyển idx_link_design_ff ... thành chữ cái cột (A, B, C, ...)
                col_link_design_letter = gspread.utils.rowcol_to_a1(1, idx_link_design_ff + 1).replace("1", "")
                col_has_design_letter = gspread.utils.rowcol_to_a1(1, idx_has_design_link_ff + 1).replace("1", "")
                col_not_design_letter = gspread.utils.rowcol_to_a1(1, idx_not_ff + 1).replace("1", "")

                # Thêm request để cập nhật 3 ô: Link Design, Has Design Link="done", Note
                update_requests.append({
                    "range": f"{col_link_design_letter}{row_number_ff}",
                    "values": [[link_design_ds]]
                })
                update_requests.append({
                    "range": f"{col_has_design_letter}{row_number_ff}",
                    "values": [["done"]]
                })
                update_requests.append({
                    "range": f"{col_not_design_letter}{row_number_ff}",
                    "values": [[note_ds]]
                })

            # Thực hiện batch_update nếu có request
            if update_requests:
                sheet_ff.batch_update(update_requests)
                print(f"✅ Đã cập nhật {len(update_requests)//3} hàng trong 'FF' với Link Design và đánh dấu 'done'.")
            else:
                print("⚠️ Không có hàng nào cần cập nhật trong 'FF'.")
        except Exception as e:
            print(f"❌ Lỗi khi import design: {e}")

    def apply_formula_to_cells(self, sheet, column_letter):
        """
        Gán công thức IMAGE() vào cột column_letter với link ảnh là ô ngay bên phải nó.
        :param sheet: Google Sheet cần chỉnh sửa.
        :param column_letter: Vị trí của cột cần gán công thức (Ví dụ: 'AC').
        """
        try:
            data = sheet.get_all_values()
            num_rows = len(data)  # Tổng số dòng có dữ liệu

            if num_rows <= 1:
                print(f"❌ Không có đủ dữ liệu trong sheet để gán công thức.")
                return
            start_row = 2  # Bắt đầu từ dòng 2
            end_row = num_rows  # Dòng cuối cùng có dữ liệu
            # Xác định cột bên phải chứa link ảnh
            col_index = gspread.utils.a1_to_rowcol(column_letter + "1")[1]  # Lấy chỉ số cột (VD: 'AC' → 29)
            adjacent_col_letter = gspread.utils.rowcol_to_a1(1, col_index  -1).replace("1", "")  # Lấy cột bên trái (VD: 'AD')

            # Xác định phạm vi ô (từ dòng 2 đến num_rows)
            cell_range = f"{column_letter}2:{column_letter}{num_rows}"

            # Tạo danh sách công thức theo từng dòng (VD: =IMAGE(AD2))
            formulas = [[f'=IMAGE({adjacent_col_letter}{i})'] for i in range(start_row, end_row + 1)]

            # Ghi công thức vào Google Sheets
            sheet.update(cell_range, formulas,value_input_option="USER_ENTERED")
            print(f"✅ Công thức đã được gán vào cột {column_letter} ({cell_range}) với link ảnh từ cột {adjacent_col_letter}.")
        except Exception as e:
            print(f"❌ Lỗi khi gán công thức vào {column_letter}: {e}")

    



