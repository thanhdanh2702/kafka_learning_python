# Kafka Data Engineer Learning Scaffold

Đây là **khung project**, không phải project đã implement xong.

- Các file cấu hình đã có nội dung và có thể dùng để khởi động môi trường.
- Các file Python và SQL theo Stage 2-10 được cố ý để trống.
- Hãy viết test trước, sau đó implement từng stage theo roadmap.

Roadmap:
[`KAFKA_DATA_ENGINEER_PYTHON_ROADMAP.md`](KAFKA_DATA_ENGINEER_PYTHON_ROADMAP.md)

## Bắt đầu

```bash
cp .env.example .env
```

Thay `POSTGRES_PASSWORD` trong `.env`, sau đó:

```bash
make validate
make up
make topics
```

`topic-init` kết thúc với trạng thái `Exited (0)` là bình thường. Service này chỉ
tạo topic một lần rồi dừng.

Kafbat UI có tại [http://localhost:8080](http://localhost:8080). Nếu thay đổi
`KAFBAT_UI_HOST_PORT` trong `.env`, hãy dùng port tương ứng.

## Quy tắc học

1. Mở `PROJECT_STRUCTURE.md` để tìm file của stage đang học.
2. Viết test vào thư mục `tests/` tương ứng.
3. Chạy test và xác nhận RED.
4. Viết implementation nhỏ nhất.
5. Chạy test và xác nhận GREEN.
6. Làm thí nghiệm lỗi theo roadmap.

Không implement nhiều stage cùng lúc.

## Lưu ý dữ liệu

```bash
make down
```

chỉ dừng container và giữ volume.

```bash
make clean
```

xóa cả Kafka log và PostgreSQL data của lab.
