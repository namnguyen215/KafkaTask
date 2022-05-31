# KafkaTask
1. Kafka
   Cụm kafka : 10.3.68.20:9092, 10.3.68.21:9092, 10.3.68.23:9092, 10.3.68.26:9092, 10.3.68.28:9092, 10.3.68.32:9092, 10.3.68.47:9092, 10.3.68.48:9092, 10.3.68.50:9092, 10.3.68.52:9092

Topic : rt-queue_1

Schema: các e cần quan tâm 3 trường {time(0), guid(6), bannerId(4)}. Các trường trong từng mesage sẽ tách nhau bởi "\t".

*   Với time: thời gian tạo record mà user tiếp cận được bannerId.
*   Với guid: id  định danh người dùng. mỗi người có một guid duy nhất.
*   bannerId: cái id banner đã hiện khi e vào website.
2. Bài toán
   Bài toán đếm số lượng user theo từng banner trong một Khoảng thời gian cụ thể.

Ví dụ : a muốn tìm số người đã xem banner 1212xx trong 2 ngày 19,20.
Ket qua : 1212xx trong 2 ngày 19,20 sẽ có khoảng 1triệu user.

Chú ý:
* Thời gian được tính một ngày là thời gian từ 6 giờ hôm nay đến 6 giờ hôm sau. ví dụ: thời gian user vào ngày 19 là khoảng thời gian bắt đầu từ 6 giờ sáng nghày 19- 6h sáng ngày 20
* Sừ dụng thuật toán hyperloglog để ước lượng user. (lấy thư viện java viết sẵn rồi mà quất)

# Tư tưởng:
1. Yêu cầu bài toán:
- Đếm số lượng user trong từng banner 

-> Đếm số các giá trị riêng biệt trong một tập dữ liệu
2. Thuật toán HyperLogLog:
- Ước lượng số lượng các giá trị riêng biệt trong một tập dữ liệu rất lớn với sai số nhỏ.


Việc sử dụng HLL trong bài toán này là chấp nhận được vì với lượng dữ liệu lớn (hàng chục triệu bản ghi)
thì việc tính toán chính xác không quan trọng bằng việc tính toán nhanh với sai số nhỏ (2-3%)

3. Áp dụng vào bài toán:
- Nhận data từ Kafka, lọc ra các cột cần thiết và đẩy vào HDFS theo các ngày
- Query từ HDFS, sử dụng HLL để ước lượng số guid theo bannerId

