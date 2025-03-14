from datetime import datetime
import gspread
import re
from oauth2client.service_account import ServiceAccountCredentials

class GoogleSheetHandler:
    def __init__(self, sheet_id):
        """Khởi tạo với ID Google Sheet"""
        self.sheet_id = sheet_id
        # self.client = gspread.Client(auth=None)  # Không cần xác thực, chỉ truy cập Google Sheet công khai
        self.client = self.authenticate_google_sheets()

    def authenticate_google_sheets(self):
        """Xác thực Google Sheets API"""
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(r"C:\Lay_link\linkDesign\credentials.json", scope)
        return gspread.authorize(creds)

    def get_sheets(self):
        """Truy xuất Sheet1 và Sheet2 từ Google Sheets"""
        sheet = self.client.open_by_key(self.sheet_id)
        return sheet.worksheet("Sheet1"), sheet.worksheet("Sheet2")
    
    
    
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

    def check_link_template(self,sheet_data,sku) :
        try:
            for row in sheet_data:
                if row[0] == sku and sku and row[0] :
                    return row[1]                
            return ""

        except Exception as e:
            print(f"❌ Lỗi khi tìm Link template cho SKU {sku} : {e}")

        return ""  # Không tìm thấy, trả về chuỗi rỗng
    

    def extract_slug(self,url):
        remove_words = {"luxinshoes", "davidress", "onesimpler", "xanawood", "lovasuit", "luxinhoes"}
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
                col_L_idx = headers.index("Link Template")  # Cột L
                col_N_idx = headers.index("Link Design")  # Cột N
                # col_M_idx = headers.index("Check Design") #cột M
            except ValueError:
                print("❌ Không tìm thấy cột K, L hoặc N trong sheet.")
                return

            # Duyệt từ trên xuống để kiểm tra
            updates = []
            for i in range(1, len(data) - 1):  # Bỏ qua dòng tiêu đề
                K_value = self.extract_slug(data[i][col_K_idx]) 
                L_value = data[i][col_L_idx]
                print('data' , i,'----', K_value,L_value )
                if not data[i][col_N_idx] :        
                    for j in range(i + 1, len(data)):  # Kiểm tra các hàng bên dưới
                        if self.extract_slug(data[j][col_K_idx])  == K_value and data[j][col_L_idx] == L_value:
                            if data[j][col_N_idx]:  # Nếu cột N của hàng bên dưới có giá trị
                                updates.append((i + 1, col_N_idx + 1, data[j][col_N_idx]))  # Lưu cập nhật
                                break

            # Thực hiện cập nhật vào Google Sheet
            if updates:
                batch_update_requests = [
                    {"range": f"N{row}", "values": [[value]]} for row, _, value in updates
                ]
                sheet.batch_update(batch_update_requests)
                print(f"✅ Đã cập nhật {len(updates)} hàng trong cột N.")

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
        design_sheet = self.client.open_by_key("1Y_EnKwWThJaxLaLQyAWGojCjcahJscZPCve5qHbwGIs")
        shoes_sheet = design_sheet.worksheet("Shoes")
        shoes_data = shoes_sheet.get_all_values()
        cn_sheet = design_sheet.worksheet("CN")
        cn_data = cn_sheet.get_all_values()

        template_sheet = self.client.open_by_key("1Uw8FQVI2ef4ANZX8pEPEO19oDSrJBzpnRIIGcV7kmUM")
        sheet_template = template_sheet.worksheet("Sheet1")
        data_template = sheet_template.get_all_values()

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
                sheet2.update("A1",new_data)
                self.apply_formula_to_cells(sheet2,"J")
                return
        else : 
            dest_order_map = {}
            for j in range(1,len(data2)) :
                dest_order_id = data2[j][1]
                dest_link_design = data2[j][13]
                dest_check_design = data2[j][12]
                if dest_order_id:
                    dest_order_map[dest_order_id] = {"row" : j +1 ,"Link Design" : dest_link_design,"Check Design":dest_check_design }
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
                if order_id in dest_order_map :
                    dest_row_index = dest_order_map[order_id]["row"]
                    dest_link_design = dest_order_map[order_id]["Link Design"]
                    dest_check_design = dest_order_map[order_id]["Check Design"]
                    link_design = self.link_design_mf(order_id,shoes_data,cn_data)
                    if link_design and (link_design != dest_link_design or not dest_check_design): 
                        updated_rows.append({"row" : dest_row_index, "value" : link_design})
                else : 
                    if order_status != "failed" and sku != "AODAU":
                        link_design = self.link_design_mf(order_id,shoes_data,cn_data)
                        check_design = "da ff" if link_design else '' 
                        new_rows.append([order_date,order_id,note,custom_name,custom_number,sku,store,type ,link_image,"",link_url,"",check_design,link_design])
            update_requests = []
            for update in updated_rows:
                update_requests.append({
                    "range": f"M{update['row']}:N{update['row']}",  # Cập nhật cả cột M và N
                    "values": [["da ff", update["value"]]]
                    # "range": f"N{update['row']}",  # Cột "N" = cột 14
                    # "values": [[update["value"]]]
                })
            sheet2.batch_update(update_requests)
            if new_rows :
                sheet2.append_rows(new_rows)
                print(f"✅ Đã thêm {len(new_rows)} đơn hàng mới vào Sheet2.")
            self.sort_sheet(sheet2,  0)
            self.apply_formula_to_cells(sheet2,"J")
        sheet1Update, sheet2Update = self.get_sheets()
        data2update = sheet2Update.get_all_values()
        update_template = []
        for i in range(1,len(data2update)) : 
            if  not data2update[i][11] : 
                link_template = self.check_link_template(data_template,data2update[i][5])
                if(link_template) : 
                    update_template.append({
                        "range": f"L{i+1}",  # Cột 12 (L)
                        "values": [[link_template]]
                    })

                if not data2update[i][13] : pass
        if update_template : 
            sheet2Update.batch_update(update_template)
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
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")  # Định dạng ngày chính xác
            except ValueError:
                return datetime.min  # Nếu lỗi, đưa về ngày nhỏ nhất (để xếp cuối)

        # ✅ Sắp xếp dữ liệu theo ngày (mới nhất -> cũ nhất)
        sorted_data = sorted(data[1:], key=lambda x: parse_date(x[sort_col]), reverse=True)

        # ✅ Xóa dữ liệu cũ và cập nhật dữ liệu mới
        sheet.clear()
        sheet.append_rows([headers] + sorted_data)
        
        print(f"✅ Đã sắp xếp Sheet theo cột {headers[sort_col]} (Ngày mới nhất -> cũ nhất).")

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

    



