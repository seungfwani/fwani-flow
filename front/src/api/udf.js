import axios from "axios";

const API_BASE_URL = "http://localhost:5050/api/v1"; // 실제 API 주소에 맞게 변경

export async function fetchUDFList() {
    try {
        const response = await axios.get(`${API_BASE_URL}/udf`);
        console.log(response.data)
        return response.data.data;
    } catch (error) {
        console.error("Failed to fetch UDF list:", error);
        return [];
    }
}

export async function uploadUDFFile(files) {
    try {
        const formData = new FormData();
        formData.append("files", files);

        const response = await axios.post("http://localhost:5050/api/v1/udf", formData, {
            headers: {
                "Content-Type": "multipart/form-data",
            },
        });

        if (response.data.success) {
            console.log("✅ UDF 업로드 성공: ", response.data.data);
        } else {
            console.log("❌ UDF 업로드 실패: ", response.data.message);
        }
        return true;
    } catch (error) {
        console.error("❌ UDF 업로드 실패:", error);
        return false;
    }
}
