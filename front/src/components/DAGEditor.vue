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
import {defineProps, ref, watch} from 'vue'
import {useVueFlow, VueFlow} from '@vue-flow/core'
import FlowSideBar from "@/components/FlowSideBar.vue";
import useDragAndDrop from "@/scripts/useDnD";
import DropzoneBackground from "@/components/DropzoneBackground.vue";
import {fetchDAGDetail, saveDAGToServer} from "@/api/dag";

const props = defineProps({
  dagId: {type: String, default: null}
})

const {nodes, edges, addNodes, addEdges, onConnect, setNodes, setEdges} = useVueFlow()

const {onDragOver, onDrop, onDragLeave, isDragOver, resetId} = useDragAndDrop()
const dagName = ref("")
const dagDescription = ref("Generated DAG")

const loadDag = async (id) => {
  if (!id) return
  // ⭐ 기존 노드/엣지 초기화!
  setNodes([])
  setEdges([])
  const dagData = await fetchDAGDetail(id)

  // 노드를 다 불러온 후, 현재 DAG의 가장 높은 node id 인덱스를 찾는다
  const maxNodeNumber = dagData.nodes
      .map((n) => {
        const match = n.id.match(/dndnode_(\d+)/)
        return match ? Number(match[1]) : 0
      })
      .reduce((a, b) => Math.max(a, b), 0)

  resetId(maxNodeNumber + 1)  // 여기서 id 시작 위치를 세팅!

  dagName.value = dagData.name
  addNodes(dagData.nodes.map(node => ({
    id: node.id,

    type: node.ui_type,
    position: node.position,
    style: node.style,
    data: {label: node.function_name, function_id: node.function_id},
    sourcePosition: 'right',
    targetPosition: 'left',
  })));
  addEdges(dagData.edges.map(edge => ({
    id: `${edge.source}-${edge.target}`,
    source: edge.source,
    target: edge.target,
    markerEnd: 'arrowclosed',
  })))
}

watch(() => props.dagId, (newId, oldId) => {
  if (newId && newId !== oldId) {
    loadDag(newId)
  }
})
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
      function_name: node.data.label,
      inputs: {},
      ui_type: node.type,
      position: node.position,
      style: node.style,
    })),
    edges: edges.value.map((edge) => ({
      source: edge.source,
      target: edge.target
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