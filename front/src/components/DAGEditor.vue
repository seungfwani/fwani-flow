<template>
  <div class="dnd-flow" @drop="onDrop">
    <div class="dnd-header">
      <h2>Dag Editor</h2>
      <input v-model="dagName" placeholder="Enter DAG Name"/>
      <button @click="saveDAG">DAG 저장</button>
    </div>
    <VueFlow :nodes="nodes" :edges="edges" @dragover="onDragOver" @dragleave="onDragLeave">
      <DropzoneBackground
          :style="{
          backgroundColor: isDragOver ? '#e7f3ff' : 'transparent',
          transition: 'background-color 0.2s ease',
        }"
      >
        <p v-if="isDragOver">Drop here</p>
      </DropzoneBackground>
    </VueFlow>

    <FlowSideBar/>
  </div>
</template>

<script setup>
import {ref} from 'vue'
import {useVueFlow, VueFlow} from '@vue-flow/core'
import FlowSideBar from "@/components/FlowSideBar.vue";
import useDragAndDrop from "@/scripts/useDnD";
import DropzoneBackground from "@/components/DropzoneBackground.vue";
import {saveDAGToServer} from "@/api/dag";

const {nodes, edges, onConnect, addEdges} = useVueFlow()

const {onDragOver, onDrop, onDragLeave, isDragOver} = useDragAndDrop()

const dagName = ref("")
const dagDescription = ref("Generated DAG")

onConnect(addEdges)

// DAG 저장
const saveDAG = async () => {
  console.log(nodes, edges)
  const dagData = {
    name: dagName.value,
    description: dagDescription.value,
    nodes: nodes.value.map((node) => ({
      id: node.id,
      function_id: node.data.function_id,
    })),
    edges: edges.value.map((edge) => ({
      from: edge.source,
      to: edge.target
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

</script>

<style>
@import "@/scripts/editer.css";
</style>