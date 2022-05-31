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

# Xử lý bài toán:
1. Yêu cầu bài toán:
- Đếm số lượng user trong từng banner 

-> Đếm số các giá trị riêng biệt trong một tập dữ liệu

2. Thuật toán HyperLogLog:
- Ước lượng số lượng các giá trị riêng biệt trong một tập dữ liệu rất lớn với sai số nhỏ.


Việc sử dụng HLL trong bài toán này là chấp nhận được vì với lượng dữ liệu lớn (hàng chục triệu bản ghi)
thì việc tính toán chính xác không quan trọng bằng việc tính toán nhanh với sai số nhỏ (2-3%)

3. Áp dụng vào bài toán:
- Nhận data từ Kafka, lọc ra các cột cần thiết và đẩy vào HDFS theo các ngày
   +Data có dạng
   ![image](https://user-images.githubusercontent.com/73151391/171086614-dbf94c6f-1feb-43bc-b184-0b7224f17fe1.png)
   
   +Cấu trúc thư mục 
   ![image](https://user-images.githubusercontent.com/73151391/171086103-06c23d79-1ed6-48b8-a0f3-4bef9ec8fed1.png)

- Query từ HDFS, sử dụng HLL để ước lượng số guid theo bannerId

![image](https://user-images.githubusercontent.com/73151391/171086770-ed7380e2-bbf5-46e5-91fb-84370bab2dc5.png)


