# TDD evidence - roadmap-aligned empty scaffold

## User journeys

1. Là người học Kafka, tôi muốn project có configuration chạy Docker để bắt đầu
   lab mà chưa phải tự dựng hạ tầng.
2. Là người học theo roadmap, tôi muốn mỗi stage có đúng file Python/SQL tương
   ứng để biết code cần viết ở đâu.
3. Là người học theo TDD, tôi muốn các file implementation ban đầu thật sự trống
   để tự viết test và code theo từng stage.

## RED

Command:

```text
python3 -m unittest discover -s tests -v
```

Kết quả trước khi tạo scaffold:

```text
Ran 3 tests
FAILED (failures=40)
```

Các failure đều do configuration hoặc implementation placeholder chưa tồn tại.

## GREEN

Cùng command sau khi tạo scaffold:

```text
Ran 3 tests in 0.001s
OK
```

## Guarantees

| Guarantee | Validation | Result |
|---|---|---|
| Configuration bắt buộc tồn tại và không rỗng | `test_configuration_files_exist_and_are_not_empty` | PASS |
| Toàn bộ Python implementation Stage 2-10 tồn tại và rỗng | `test_python_implementation_files_exist_and_are_empty` | PASS |
| Bốn SQL migration Stage 7 tồn tại và rỗng | `test_sql_implementation_files_exist_and_are_empty` | PASS |
| Compose hợp lệ với environment mẫu | `docker compose --env-file .env.example config --quiet` | PASS |
| Topic provisioning script đúng shell syntax | `bash -n scripts/create-topics.sh` | PASS |
| Python placeholder và structure test compile được | `python3 -m compileall -q src tests` | PASS |

## Coverage

Python standard-library `trace` báo:

```text
test_project_structure: 31 executable lines, 96% covered
```

Implementation coverage chưa áp dụng vì các file implementation được yêu cầu để
trống. Mỗi stage phải bổ sung test mới trước khi điền code vào placeholder.

## Runtime gap

Không tải image hoặc khởi động container trong bước scaffold. Runtime E2E đầu
tiên là `make up` sau khi người dùng tạo `.env` và đặt local password.
