# Tổng quan

## Hướng dẫn Setup môi trường
1. Khởi tạo GCP project
    
Tại màn hình console của GCP, chọn hoặc tạo project mới.

Điền tên project sau đó chọn `Create`.

![](images/image1.png)

Tại màn hình console chính, nhấn vào biểu tượng `Navigation menu` -> `Billing` -> `Link a billing account` để liên kết tài khoản thanh toán với project.

![image2.png](images/image2.png)
   
2. Enable các API Services cần thiết

Nhấn vào biểu tượng `Navigation menu` -> `APIs & Services` -> `Enable APIs and Services`.
Chọn `ENABLE APIS AND SERVICES`, tìm kiếm và `ENABLE` các API sau:
   - Google Drive API
   - Compute Engine API

3. Tạo Service Account và tải file json key

Service Account là một loại tài khoản đặc biệt của Google, không đại diện cho bất cứ người dùng cụ thể nào. Service Account được sử dụng để thực hiện xác thực và phân quyền cho các ứng dụng cần truy cập vào tài nguyên của GCP. Cùng với cơ chế Role-based Access Congtrol (Kiểm soát truy cập dựa trên vai trò), mỗi Role tương ứng với một tập hợp các quyền (permissrions) cho phép thực hiện các thao tác lên tài nguyên GCP. Mỗi Service Account có thể được gán một hoặc nhiều Role, và mỗi Role có thể được gán cho một hoặc nhiều Service Account. Các Role được gán cho một Service Account sẽ xác định các quyền mà Service Account đó có thể thực hiện trên các tài nguyên của GCP.
   
Để tạo Service Account, nhấn vào biểu tượng `Navigation menu` -> `IAM & Admin` -> `Service Accounts` -> `Create Service Account`.

![](images/image3.png)

Điền tên Service Account, chọn `CREATE AND CONTINUE`.
Lần lượt tìm và chọn các Role sau:
   - Cloud Storage -> Storage Admin
   - Cloud Storage -> Storage Object Admin
   - Big Query -> BigQuery Data Owner
   - Big Query -> BigQuery User

![image4.png](images/image4.png)

Chọn `DONE` để hoàn tất tạo Service Account.

Tại màn hình `Service Accounts`, tìm tới Service Account vừa tạo,  nhấn vào biểu tượng `Actions` -> `Manage keys`.

![image5.png](images/image5.png)

Chọn `ADD KEY` -> `Create new key` -> `JSON` -> `CREATE` để tạo file json key và tải về máy.

4. Tạo Google Cloud Storage Bucket

Nhấn vào biểu tượng `Navigation menu` -> `Cloud Storage` -> `Buckets` -> `Create`.

Điền tên bucket và nhấn `CONTINUE`.

Chọn như hình dưới và nhấn `CREATE`.

![image6.png](images/image6.png)

Cửa sổ Confirm `Public access will be prevented` sẽ hiện ra, chọn `CONFIRM` để xác nhận chặn truy cập public và hoàn tất tạo bucket.

Default Storage Class và Location có thể chọn theo nhu cầu sử dụng.
Trong đó Storage Class là metadata của object, xác định cách lưu trữ và giá cả của object. Location là vị trí lưu trữ của object, ảnh hưởng đến tốc độ truy cập và giá cả.
Có 4 loại Storage Class:
   - **Standard**: Lưu trữ dữ liệu trên nhiều thiết bị và nhiều vị trí, đảm bảo độ tin cậy và khả năng khôi phục dữ liệu cao nhất. Giá cả cao nhất.
   - **Nearline**: Dành cho dữ liệu ít truy cập, giá cả thấp hơn Standard nhưng phí truy cập cao hơn.
   - **Coldline**: Dành cho dữ liệu ít truy cập, giá cả thấp nhất nhưng phí truy cập cao nhất.
   - **Archive**: Dành cho dữ liệu ít truy cập, giá cả thấp nhất nhưng phí truy cập cao nhất, thời gian truy cập dữ liệu lâu nhất.

Để giúp tối ưu chi phí lưu trữ, Goole cung cấp cơ chế Object Lifecycle Management giúp tự động chuyển đổi giữa các loại Storage Class dựa trên các Rule xác định.

Trong dự án này, dữ liệu được download từ Google Drive lưu trữ trên Cloud Storage Bucket hàng ngày, sau đó dữ liệu được insert vào BigQuery để phục vụ nhu cầu phân tích. 
Để tối ưu chi phí lưu trữ trong dự án này, ta sẽ sử dụng thiết lập các Rule như sau:
- Dữ liệu được download từ GG Drive sau đó ngay lập tức được insert vào BigQuery -> Default Storage Class: Standard
- Dữ liệu được insert vào BigQuery sẽ không còn cần lưu trữ trên Cloud Storage, tuy nhiên để tránh trường hợp quá trình insert từ GCS -> BigQuery gặp lỗi dẫn tới phải download lại dữ liệu từ GG Drive, ta sẽ lưu trữ dữ liệu trên GCS trong 7 ngày sau đó tự động xóa object khỏi GCS -> Rule: 7 ngày sau khi object được tạo, xóa object.

Tùy thuộc vào các yêu cầu cụ thể cũng như mục tiêu sử dụng, tần suất truy cập và kích thước dữ liệu, ta có thể lựa chọn Storage Class và thiết lập Rule phù hợp một cách linh hoạt.

Để thiết lập Object Lifecycle Management, nhấn vào bucket vừa tạo, chọn `Lifecycle` -> `Add A rule`.

Điền thông tin Rule như hình dưới và nhấn `CREATE`.

![image7.png](images/image7.png)

![image8.png](images/image8.png)

5. Setup Airflow trên GCP Compute Engine

Nhấn vào biểu tượng `Navigation menu` -> `Compute Engine` -> `VM instances` -> `CREATE INSTANCE`.

Điền thông tin như hình dưới và nhấn `CREATE`.

![image9.png](images/image9.png)

![image10.png](images/image10.png)

Chọn machine type:

![image11.png](images/image11.png)

Chọn boot disk:

![image12.png](images/image12.png)

![image13.png](images/image13.png)

Chon `Create` để tạo instance.

Để có thể truy cập vào Airfow Web UI, ta cần thiết lập Firewall Rule cho instance vừa tạo.

Tại màn hình `VM instances`, chọn instance vừa tạo, chọn `More Action` -> `View network details`.

![image14.png](images/image14.png)

Tại section `Network interface details` -> click vaò network `default`

![image15.png](images/image15.png)

Chọn `Firewall` -> `Add firewall rule`.

![image16.png](images/image16.png)

Thiết lập thông tin như hình dưới và nhấn `CREATE`.

Quay trở lại màn hình `Compute Engine` -> `VM instances`, chọn instance vừa tạo, chọn `SSH` để truy cập vào instance.

Cửa sổ terminal hiện ra, thực hiện các bước sau: 
 - Setup Docker apt repository:
```bash
# Add Docker's official GPG key:
sudo apt-get update
sudo apt-get install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```
- Cài đặt Docker:
```bash
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

- Kiểm tra Docker đã cài đặt thành công:
```bash
sudo docker --version
```

- Cài đặt git và make:
```bash
sudo apt-get install git make
```

- Clone repository:
```bash
git clone https://github.com/NgQuangHuyit/GCP-Data-Pipeline.git
```
- Setup project:
```bash
cd GCP-Data-Pipeline
make init-dirs
make setup
```

Nhập vào lần lượt các thông tin sau:
- Airfow Webserver Username. Ex: airflow
- Airfow Webserver Password Ex: airflow
- GCS Bucket Name (Tên bucket đã tạo) Ex: mybucket
- BigQuery dataset name Ex: mydataset
- BigQuery table name Ex: mytable

Chọn `UPLOAD` để upload file json key đã tạo ở bước 3.O.

Di chuyển file json key vào thư mục `credentials`:
```bash
mv ~/<json-file-name> ~/GCP-Data-Pipeline/credentials/
```

Chaỵ docker compose:
```bash
sudo make up
```

# DEMO
Truy cập vào Airflow Web UI bằng cách mở trình duyệt và truy cập vào địa chỉ `http://<external-ip>:8081` với <external-ip> là external ip của instance. Tài khoản và mật khẩu để truy cập vào Airflow Web UI là thông tin đã nhập ở bước 5.1.

![airflowui.png](images/airflowui.png)

Chọn `nexar-pipeline` -> click vào `Trigger Button` để chạy pipeline.

![dag.png](images/dag.png)

GCS bucket:

![bucket1.png](images/bucket1.png)

![bucket2.png](images/bucket2.png)

BigQuery:

![bigquery1.png](images/bigquery1.png)  

Truy vấn dữ liệu từ BigQuery:

![bigquery2.png](images/bigquery2.png)