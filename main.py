from google_sheet import GoogleSheetHandler


def main():
    print('khoi dong chuong trinh ....')
    source_sheet_ids = ["1iU5kAhVSC0pIP2szucrTm4PaplUh501H2oUvLgx0mw8",
        "1cGF0JBFX1dkTq_56-23IblzLKpdqgVkPxNb-ZX5-sQA",
        "1j5VHpm1g3hlXK-HncynZNybubWLLmlsWt-rK5ws9UFM",
        # "1CmmjO1NVG8hRe6YaurCHT4Co3GhSw39ABIwwTcv4sHw",
        "1oTKNUs_3XRJ7GD4C8q5ay-1JjRub2wKdOF1HDFSXEo8",
        # "15sEghfR8L-_leRNhSz62K--jtWFZPn-ix6BH0MuLIB0" 
    ]  # Danh sách ID sheet nguồn
    destination_sheet_id = "1rzAqanj3oekf-b_jAyAQL9dXZ2b374aGLfz1-6mPomw"  # ID của sheet đích
    # Khởi tạo GoogleSheetHandler
    handler = GoogleSheetHandler(destination_sheet_id)
    # Lấy dữ liệu từ nhiều Sheet1 của các file nguồn
    data = handler.copy_all_data_sheets(source_sheet_ids)
    print('wordsheet')
    if data:
        # Kết nối với Google Sheet đích
        destination_sheet = handler.client.open_by_key(destination_sheet_id)
        wordsheet = destination_sheet.worksheet("Sheet1")
        wordsheet.clear()  # Xóa dữ liệu cũ
        wordsheet.update("A1", data)  # Ghi dữ liệu từ A1
        handler.sort_sheet(wordsheet,0)
        handler.apply_formula_to_cells(wordsheet,"AC")
        handler.update_sheet2()
        handler.generate_sheet3()
        print("Dữ liệu từ nhiều Google Sheet đã được sao chép thành công!")
    else:
        print("Không có dữ liệu để sao chép.")

if __name__ == "__main__":
    main()