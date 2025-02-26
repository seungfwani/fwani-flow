<template>
  <div class="dag-editor">
    <!-- 🏷️ 상단 헤더 -->
    <div class="dag-header">
      <h2>DAG Editor</h2>
      <input v-model="dagName" placeholder="Enter DAG Name" />
      <button @click="saveDAG">💾 DAG 저장</button>
    </div>

    <!-- 🏷️ Vue-Flow DAG 편집기 (크기 고정) -->
    <div class="dag-container-wrapper">
      <vue-flow
        v-model="elements"
        class="dag-container"
        :default-zoom="1"
        :min-zoom="0.5"
        :max-zoom="2"
        :node-types="nodeTypes"
        :edges-updatable="true"
        :nodes-connectable="true"
        :edges-draggable="true"
        @dragover.prevent
        @drop="onDrop"
      >
        <background variant="dots" :gap="15" :size="1" />
        <controls />
      </vue-flow>
    </div>

    <!-- UDF 목록 -->
    <div class="udf-list">
      <h3>🧩 사용 가능한 UDF</h3>
      <div
        v-for="udf in udfList"
        :key="udf.id"
        class="udf-item"
        draggable="true"
        @dragstart="startDrag($event, udf)"
      >
        {{ udf.name }}
      </div>
    </div>
  </div>
</template>

<script>
import { ref, onMounted } from "vue";
import { VueFlow, Controls, Background, useVueFlow } from "@vue-flow/core";
import CustomNode from "@/components/CustomNode.vue";
import { fetchUDFList } from "@/api/udf.js";
import { saveDAGToServer } from "@/api/dag.js";

export default {
  components: { VueFlow, Controls, Background },
  setup() {
    const { addNodes, addEdges } = useVueFlow();
    const elements = ref([]);
    const udfList = ref([]);
    const nodeIdCounter = ref(1);
    const dagName = ref("");

    // Vue-Flow 커스텀 노드 적용
    const nodeTypes = { customNode: CustomNode };

    // UDF 목록 불러오기
    const loadUDFs = async () => {
      udfList.value = await fetchUDFList();
    };

    // UDF 드래그 시작
    const startDrag = (event, udf) => {
      event.dataTransfer.setData("udf", JSON.stringify(udf));
    };

    // DAG 에디터에 UDF 드롭
    const onDrop = (event) => {
      event.preventDefault();
      const udfData = event.dataTransfer.getData("udf");
      if (!udfData) return;
      const udf = JSON.parse(udfData);

      const bounds = event.currentTarget.getBoundingClientRect();
      const position = {
        x: event.clientX - bounds.left,
        y: event.clientY - bounds.top,
      };

      console.log(`🟢 UDF 드롭됨: ${udf.name} at ${position.x}, ${position.y}`);

      addNodes({
        id: `task_${nodeIdCounter.value}`,
        type: "customNode",
        label: udf.name,
        position,
        data: {
          function_id: udf.id,
          filename: udf.filename,
          function: udf.function,
        },
      });

      nodeIdCounter.value++;
    };

    // Edge 추가 기능 (노드 간 선 연결)
    const addEdge = (source, target) => {
      const newEdge = { id: `edge-${source}-${target}`, source, target };
      addEdges([newEdge]);
      console.log("🔗 Edge 추가됨:", newEdge);
    };

    // DAG 저장
    const saveDAG = async () => {
      const dagData = {
        name: dagName.value,
        description: "Generated DAG",
        nodes: elements.value.map((node) => ({
          id: node.id,
          function_id: node.data.function_id,
        })),
        edges: elements.value
          .filter((el) => el.source && el.target)
          .map((edge) => ({
            from: edge.source,
            to: edge.target,
          })),
      };

      console.log("🔄 DAG 저장 요청 데이터:", JSON.stringify(dagData, null, 2));

      const response = await saveDAGToServer(dagData);
      if (response && response.message) {
        alert(`✅ DAG 저장 완료: ${response.message}`);
      } else {
        alert("❌ DAG 저장 실패: 서버 응답이 없습니다.");
      }
    };

    onMounted(loadUDFs);

    return {
      elements,
      udfList,
      saveDAG,
      startDrag,
      onDrop,
      dagName,
      nodeTypes,
      addEdge,
    };
  },
};
</script>

<style scoped>
.dag-editor {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 10px;
}

.dag-header {
  display: flex;
  justify-content: space-between;
  width: 100%;
  padding: 10px;
  background: #eee;
  border-radius: 5px;
}

/* 🛠️ DAG 편집기 크기 고정 */
.dag-container-wrapper {
  width: 800px;
  height: 600px;
  display: flex;
  justify-content: center;
  align-items: center;
  border: 2px solid #ddd;
  border-radius: 5px;
  background: white;
}

.dag-container {
  width: 100%;
  height: 100%;
}

/* ✅ 노드 스타일 */
.custom-node {
  border: 2px solid #3498db;
  border-radius: 8px;
  padding: 10px;
  background-color: white;
  width: 150px;
  height: 50px;
  text-align: center;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
}

.handle-left {
  left: -5px;
  position: absolute;
  width: 10px;
  height: 10px;
  background: #3498db;
  border-radius: 50%;
}

.handle-right {
  right: -5px;
  position: absolute;
  width: 10px;
  height: 10px;
  background: #3498db;
  border-radius: 50%;
}

.udf-list {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 10px;
}

.udf-item {
  padding: 5px 10px;
  background: #ddd;
  border-radius: 3px;
  cursor: grab;
}
</style>