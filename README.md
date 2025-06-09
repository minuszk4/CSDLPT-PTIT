````markdown
# Hướng dẫn thiết lập dự án với PostgreSQL
````
## 1. Clone repository từ GitHub
```bash
git clone <URL-repo-của-bạn>
cd <tên-thư-mục-repo>
````

## 2. Thiết lập môi trường ảo (venv)

* Cài đặt gói hỗ trợ venv:

```bash
sudo apt install python3-venv -y
```

* Tạo môi trường ảo:

```bash
python3 -m venv venv
```

* Kích hoạt môi trường ảo:

```bash
source venv/bin/activate
```

* Cài đặt thư viện `psycopg2-binary` để kết nối PostgreSQL:

```bash
pip install psycopg2-binary
```

## 3. Thiết lập PostgreSQL

* Cài đặt PostgreSQL và tiện ích:

```bash
sudo apt install postgresql postgresql-contrib
```

* Đổi mật khẩu cho user mặc định `postgres` (ở đây đặt là `'1'`):

```bash
sudo -u postgres psql
```

Trong giao diện `psql`, chạy:

```sql
ALTER USER postgres WITH PASSWORD '1';
\q
```

## 4. Chuẩn bị dữ liệu

* `test_data.dat`: Có sẵn trong repository.
* `ratings.dat`: Tải từ MovieLens (đã có trong repository).

## 5. Chạy code

Sau khi hoàn thành các bước trên, bạn có thể chạy code dự án bình thường.

---

### Lưu ý quan trọng:

* Nếu không muốn dùng user `postgres`, có thể tạo user mới trong PostgreSQL bằng lệnh:

```bash
sudo -u postgres createuser <tên-user-mới>
sudo -u postgres psql
```

Sau đó trong `psql`:

```sql
ALTER USER <tên-user-mới> WITH PASSWORD '<mật-khẩu-mới>';
GRANT ALL PRIVILEGES ON DATABASE <tên-database> TO <tên-user-mới>;
\q
```

* Khi thay đổi user hoặc mật khẩu, nhớ cập nhật thông tin kết nối database trong file code (phần , `user`, `password`, `database`).


