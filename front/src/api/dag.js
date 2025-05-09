import axios from "axios";

const API_BASE_URL = "http://localhost:5050/api/v1"; // 실제 API 주소에 맞게 변경

export async function fetchDAGList() {
    try {
        const response = await axios.get(`${API_BASE_URL}/dag`);
        console.log(response.data)
        return response.data.data;
    } catch (error) {
        console.error("Failed to fetch DAG list:", error);
        return [];
    }
}

export async function fetchDAGDetail(id) {
    try {
        const response = await axios.get(`${API_BASE_URL}/dag/${id}`);
        console.log(response.data)
        return response.data.data;
    } catch (error) {
        console.error(`Failed to fetch DAG ${id}:`, error);
        return [];
    }
}

export const saveDAGToServer = async (dagData) => {
    try {
        console.log("🔄 DAG 저장 요청 데이터:", dagData);

        const response = await axios.post(`${API_BASE_URL}/dag`, dagData);

        if (!response || !response.data) {
            throw new Error("❌ 서버 응답 없음");
        }

        console.log("✅ DAG 저장 성공:", response.data);
        return response.data;  // 정상적인 응답 반환
    } catch (error) {
        console.error("❌ DAG 저장 실패:", error);
        throw new Error(`DAG 저장 중 오류 발생: ${error.message}`);  // 강제 예외 발생
    }
};

export const updateDAGToServer = async (id, dagData) => {
    try {
        console.log("🔄 DAG 업데이트 요청 데이터:", id, dagData);

        const response = await axios.patch(`${API_BASE_URL}/dag/${id}`, dagData);

        if (!response || !response.data) {
            throw new Error("❌ 서버 응답 없음");
        }

        console.log("✅ DAG 업데이트 성공:", response.data);
        return response.data;  // 정상적인 응답 반환
    } catch (error) {
        console.error("❌ DAG 업데이트 실패:", error);
        throw new Error(`DAG 업데이트 중 오류 발생: ${error.message}`);  // 강제 예외 발생
    }
}