from datetime import datetime
import sys
import threading
import time
from google_sheet import GoogleSheetHandler


def main():
    print('khoi dong chuong trinh ....')
    source_sheet_ids = [
        "1AygotqSY58fHQgAEVVmnpO-REwJEVvqdpxGi9u1jsn4",
        "1UiOMmQPkMmq0tewpiCsmyrXx7qMwW6iE21GjqzHVO7c",
        "14KecG--oRcj5otgvFJ8Kl16D556L9Cz32K4I3TjyBRY",
        "1LnDxYEHkJ5yxLU8KyEZhnivSYoxuYqB4b9TjoAssdSo",
        "1t55QypLzvRFUDh0BchJfU9-Y-wAQPF-06yeJ8XW-ttY",
        "1oATa0YEllGkC8aFWiElzWO0nJmp2652mhqyvq3sVnOo",
        "11vRLaxloprMzBe8hwrASOLetiVWZGwjEKBU2p8s11zo",
        "1SySSJt1i4lHp8Q3SlAE5VmsDfjEJ6oecxTABivAedW0",
        "18Y44B205GJBhgbMrhfOdcc1dcjxsujjjFkHx49cwsU0",
        "1SinUd6nxbowMmwWiZcw16yNJsprOHtEdJl1g0pxb0fM",
        "1avty1G04ugUEiS5pwJPKFW0YZr8Rh-ogyro4HajZyRc",
        "141M1T0VI6BOrsLokIxKhfzwvzSPrKgVoQKMUAwpw-Bw",
        "1Eh1DQ55AmVQcg0j8q6tFUZ9d8a8V_6ugO3uxU4n9gTw",
        "1iU5kAhVSC0pIP2szucrTm4PaplUh501H2oUvLgx0mw8",
        "1cGF0JBFX1dkTq_56-23IblzLKpdqgVkPxNb-ZX5-sQA",
        "1j5VHpm1g3hlXK-HncynZNybubWLLmlsWt-rK5ws9UFM",
        # "1CmmjO1NVG8hRe6YaurCHT4Co3GhSw39ABIwwTcv4sHw",
        "1oTKNUs_3XRJ7GD4C8q5ay-1JjRub2wKdOF1HDFSXEo8",
        # "15sEghfR8L-_leRNhSz62K--jtWFZPn-ix6BH0MuLIB0" 
    ]  # Danh sách ID sheet nguồn
    cutoff_date = datetime(2025, 6, 15)
    destination_sheet_id = "1rzAqanj3oekf-b_jAyAQL9dXZ2b374aGLfz1-6mPomw"  # ID của sheet đích
    # Khởi tạo GoogleSheetHandler
    handler = GoogleSheetHandler(destination_sheet_id)
    # Lấy dữ liệu từ nhiều Sheet1 của các file nguồn2
    data = handler.copy_all_data_sheets(source_sheet_ids)
    print("Vui lòng chọn chức năng:")
    print("1: Cập nhật các sheet (logic cũ)")
    print("2: Import Design")
    choice = [None]


    # đưa hàm đợi vào 
    def ask_choice():
        choice[0] = input("Nhập 1 hoặc 2: ").strip()

    # Khởi thread đợi input
    t = threading.Thread(target=ask_choice)
    t.daemon = True
    t.start()

    # Chờ tối đa 120 giây
    t.join(120)

    # Nếu hết 120s mà chưa nhập thì mặc định 1
    if choice[0] is None:
        choice[0] = "1"
        print("\n⏰ Hết thời gian, mặc định chọn 1.")

    # Đưa về biến đơn giản
    choice = choice[0]
    # choice = input("Nhập 1 hoặc 2: ").strip()
    if choice == "1":
        if data:
            print('bắt đầu cập nhật sheet ....')
            # Kết nối với Google Sheet đích
            destination_sheet = handler.client.open_by_key(destination_sheet_id)

            wordsheet = destination_sheet.worksheet("Sheet1")
            wordsheet_cloan = destination_sheet.worksheet("C Loan")
            time.sleep(60)
            wordsheet.clear()  # Xóa dữ liệu cũ
            wordsheet.update(range_name ="A1",values = data)  # Ghi dữ liệu từ A1
            handler.sort_sheet(wordsheet,0) 
            time.sleep(60)

            wordsheet_cloan.clear()  # Xóa dữ liệu cũ
            wordsheet_cloan.update(range_name ="A1",values = data)  # Ghi dữ liệu từ A1
            handler.sort_sheet(wordsheet_cloan,0)
            time.sleep(60)
            handler.apply_formula_to_cells(wordsheet,"AC")
            handler.update_sheet2()
            time.sleep(60)
            handler.generate_sheet3()
            time.sleep(60)
            sheet2_data = handler.copy_all_data_sheet2(source_sheet_ids)
            print("✅ Đã coppy sheet2 xong")
            time.sleep(60)
            handler.generate_sheet4(sheet2_data)
            time.sleep(60)
            handler.generate_sheet_ff()
            print("✅ Đã cập nhật sheet FF.")
            print("Dữ liệu từ nhiều Google Sheet đã được sao chép thành công!")
        else:
            print("Không có dữ liệu để sao chép.")
        sys.exit()
    if choice == "2" :
        print('Chạy Import Design ...')
        handler.import_design()
        sys.exit()
    else:
        print("Lựa chọn không hợp lệ. Vui lòng chạy lại và chọn 1 hoặc 2.")
        sys.exit()

if __name__ == "__main__":
    main()