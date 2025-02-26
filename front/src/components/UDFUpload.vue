<template>
  <div>
    <!-- UDF 추가 버튼 -->
    <button @click="showUploadModal = true" class="upload-btn">➕ UDF 추가</button>

    <!-- 모달창 -->
    <div v-if="showUploadModal" class="modal">
      <div class="modal-content">
        <h3>UDF 업로드</h3>
        <input type="file" @change="handleFileUpload" accept=".py"/>
        <button @click="uploadUDF">📤 업로드</button>
        <button @click="showUploadModal = false">❌ 닫기</button>
      </div>
    </div>
  </div>
</template>

<script>
import {ref} from "vue";
import {uploadUDFFile} from "@/api/udf.js";

export default {
  setup() {
    const showUploadModal = ref(false); // 모달 상태
    const selectedFile = ref(null);

    const handleFileUpload = (event) => {
      selectedFile.value = event.target.files[0];
      console.log("📌 선택된 파일:", selectedFile.value);
    };

    const uploadUDF = async () => {
      if (!selectedFile.value) {
        alert("📂 업로드할 파일을 선택하세요!");
        return;
      }

      const success = await uploadUDFFile(selectedFile.value);
      if (success) {
        alert("✅ UDF 업로드 완료!");
        showUploadModal.value = false;
        window.location.reload(); // 페이지 새로고침하여 UDF 리스트 갱신
      } else {
        alert("❌ 업로드 실패!");
      }
    };

    return {showUploadModal, handleFileUpload, uploadUDF};
  },
};
</script>

<style scoped>
.upload-btn {
  padding: 8px 12px;
  background-color: #4caf50;
  color: white;
  border: none;
  cursor: pointer;
  font-size: 14px;
}

.modal {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
  display: flex;
  justify-content: center;
  align-items: center;
}

.modal-content {
  background: white;
  padding: 20px;
  border-radius: 8px;
  text-align: center;
}

.modal-content input {
  margin-bottom: 10px;
}
</style>