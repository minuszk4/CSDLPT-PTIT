Clone repo về từ github
Thiết lập venv cho dự án bằng cách : 
  cài đặt sudo apt install python3-venv -y
  Tạo venv bằng lệnh python3 -m venv venv
  Kích hoạt môi trường ảo với lệnh source venv/bin/activate
  Cài đặt thư viện psycopg2-binary bằng lệnh pip install psycopg2-binary
Thiết lập postgreSQL
  Cài đặt bằng lệnh sudo apt install postgresql postgresql-contrib
  Đặt mật khẩu cho tài khoản mặc định postgres ALTER USER postgres WITH PASSWORD '1'; 
Cài đặt dữ liệu
  test_data.dat: Trong repository.
  ratings.dat: Tải từ MovieLens(trong respository).
Bây giờ có thể chạy code
Note: Nếu không muốn dùng user postgres thì có thể tạo mới, Hiện tại trong code đang sử dụng user postgres với password  =  1 nếu thay đổi user và password phải đổi mật khẩu
