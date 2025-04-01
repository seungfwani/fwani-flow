<template>
  <div>
    <button @click="showUploadModal = true" class="upload-btn">➕ UDF 추가</button>

    <div v-if="showUploadModal" class="modal">
      <div class="modal-content">

        <div class="udf-form">

          <h4>함수 정보</h4>
          <div class="form-section">

            <div class="form-row">
              <label>UDF 파일 업로드 (여러 파일 가능)</label>
              <input
                  type="file"
                  @change="handleFileUpload"
                  multiple
              />
            </div>
            <div class="form-row">
              <label>UDF 이름: </label>
              <input v-model="udfInfo.name" placeholder="UDF 이름"/>
            </div>

            <div class="form-row">
              <label>메인 파일 선택:</label>
              <select v-model="udfInfo.main_filename">
                <option disabled value="">메인 파일 선택</option>
                <option v-for="file in udfFiles" :key="file" :value="file.name">{{ file.name }}</option>
              </select>
            </div>
            <div class="form-row">

              <label>메인 함수 이름:</label>
              <select v-model="udfInfo.function_name">
                <option disabled value="">함수 선택</option>
                <option v-for="fn in extractedFunctions" :key="fn" :value="fn">{{ fn }}</option>
              </select>
            </div>

            <div class="form-row">
              <label>함수의 실행 타입:</label>
              <select v-model="udfInfo.operator_type">
                <option disabled value="">타입 선택</option>
                <option value="python">python</option>
                <option value="python_virtual">python_virtual</option>
                <option value="docker">docker</option>
              </select>

            </div>
            <div class="form-row" v-if="udfInfo.operator_type === 'docker'">
              <label>도커 이미지 이름:</label>
              <input v-model="udfInfo.docker_image" placeholder="도커 이미지 이름"/>
            </div>
          </div>

          <h4>입력 파라미터</h4>
          <div class="input-block" v-for="(input, index) in udfInfo.inputs" :key="index">
            <div class="form-row">
              <label>파라미터 {{ index }} 이름:</label>
              <input v-model="input.name" placeholder="입력 이름"/>
            </div>
            <div class="form-row">
              <label>파라미터 {{ index }} 타입:</label>
              <select v-model="input.type">
                <option disabled value="">타입 선택</option>
                <option value="string">string</option>
                <option value="int">int</option>
                <option value="float">float</option>
                <option value="bool">bool</option>
                <option value="list">list</option>
                <option value="dict">dict</option>
              </select>
            </div>
            <div class="form-row">
              <label>기본값:</label>
              <input v-model="input.default_value" placeholder="기본값"/>
            </div>
            <div class="form-row">
              <label>설명:</label>
              <input v-model="input.description" placeholder="설명"/>
            </div>
            <div class="form-row">
              <label>필수:</label>
              <input type="checkbox" v-model="input.required"/>
            </div>
            <button class="remove-btn" @click="removeInput(index)">❌ 제거</button>
          </div>

          <button class="add-btn" @click="addInput">➕ 입력 추가</button>

          <h4>출력 정보</h4>
          <div class="input-block">

            <div class="form-row">
              <label>이름:</label>
              <input v-model="udfInfo.output.name" placeholder="출력 이름"/>
            </div>

            <div class="form-row">
              <label>함수의 리턴 타입:</label>
              <select v-model="udfInfo.output.type">
                <option disabled value="">타입 선택</option>
                <option value="string">string</option>
                <option value="int">int</option>
                <option value="float">float</option>
                <option value="bool">bool</option>
                <option value="list">list</option>
                <option value="dict">dict</option>
              </select>
            </div>

            <div class="form-row">
              <label>출력 설명:</label>
              <input v-model="udfInfo.output.description" placeholder="출력 설명"/>
            </div>
          </div>
        </div>


        <button class="add-btn" @click="handleUpload">📤 업로드</button>
        <button class="remove-btn" @click="showUploadModal = false">❌ 닫기</button>
      </div>
    </div>
  </div>
</template>

<script setup>
import {ref, watch} from 'vue';
import {uploadUDFFile} from "@/api/udf";

const showUploadModal = ref(false);
const udfFiles = ref([]);
const udfInfo = ref({
  name: '',
  main_filename: '',
  function_name: '',
  operator_type: '',
  docker_image: '',
  inputs: [],
  output: {
    name: '',
    type: '',
    description: ''
  }
});
const extractedFunctions = ref([]); // 함수 이름 목록
function handleFileUpload(event) {
  udfFiles.value = Array.from(event.target.files);

  // 함수 목록 초기화 (업로드할 때마다)
  extractedFunctions.value = [];
}

watch(
    () => udfInfo.value.main_filename,
    (selectedFile) => {
      const file = udfFiles.value.find(f => f.name === selectedFile);
      if (file && file.name.endsWith('.py')) {
        const reader = new FileReader();
        reader.onload = () => {
          const code = reader.result;
          const matches = [...code.matchAll(/^def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(/gm)];
          extractedFunctions.value = matches.map(match => match[1]);
        };
        reader.readAsText(file);
      } else {
        extractedFunctions.value = [];
      }
    }
);

function addInput() {
  udfInfo.value.inputs.push({
    name: '',
    type: '',
    required: false,
    default_value: '',
    description: ''
  });
}

function removeInput(index) {
  udfInfo.value.inputs.splice(index, 1);
}

async function handleUpload() {
  const success = await uploadUDFFile(udfInfo.value, udfFiles.value);
  if (success) {
    showUploadModal.value = false;
  } else {
    alert("업로드 실패!");
  }
}
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
  padding: 10px;
  border-radius: 8px;
  text-align: center;
  width: 80vw; /* 전체 화면의 80% 너비 */
  max-width: 900px; /* 최대 900px까지만 넓어지게 */
  min-width: 300px; /* 너무 좁아지지 않게 */
  max-height: 90vh; /* 화면 높이의 90%까지만 */
  overflow-y: auto; /* 세로 스크롤 생기게 */
}

.modal-content input {
  margin-bottom: 10px;
}

.udf-form {
  max-width: 800px;
  margin: auto;
  padding: 20px;
  font-family: sans-serif;
}

.form-section label,
.input-block label {
  display: block;
  margin-bottom: 10px;
}

input,
select {
  width: 50%;
  padding: 6px;
  margin-top: 4px;
  box-sizing: border-box;
}

.form-row {
  display: flex;
  align-items: center;
  margin-bottom: 12px;
}

.form-row label {
  width: 30%;
  min-width: 120px;
  font-weight: bold;
  margin-right: 10px;
}

.form-row input,
.form-row select {
  width: 50%;
  padding: 6px;
  box-sizing: border-box;
}

.input-block {
  border: 1px solid #ddd;
  padding: 15px;
  border-radius: 6px;
  margin-bottom: 15px;
  background-color: #fafafa;
}

.remove-btn,
.add-btn {
  margin-top: 10px;
  padding: 5px 10px;
  cursor: pointer;
  background: #e74c3c;
  color: white;
  border: none;
  border-radius: 4px;
}

.add-btn {
  background: #2ecc71;
}
</style>