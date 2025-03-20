<template>
  <div class="dnd-flow" @drop="onDrop">
    <div class="dnd-header">
      <h2>Dag Editor</h2>
      <input v-model="dagName" placeholder="Enter DAG Name"/>
      <button @click="saveDAG">DAG 저장</button>
    </div>
    <VueFlow :nodes="nodes"
             :edges="edges"
             @dragover="onDragOver"
             @dragleave="onDragLeave"
             @node-click="onNodeClick"
    >
      <DropzoneBackground
          :style="{
          backgroundColor: isDragOver ? '#e7f3ff' : 'transparent',
          transition: 'background-color 0.2s ease',
        }"
      >
        <p v-if="isDragOver">Drop here</p>
      </DropzoneBackground>
    </VueFlow>

    <TaskInputModal
        :visible="showInputModal"
        :node="selectedNode"
        @close="showInputModal = false"
        @save="saveNodeInputs"
    />
    <FlowSideBar/>
  </div>
</template>

<script setup>
import {defineEmits, defineProps, ref, watch} from 'vue'
import {useVueFlow, VueFlow} from '@vue-flow/core'
import FlowSideBar from "@/components/FlowSideBar.vue";
import TaskInputModal from "@/components/TaskInputModal.vue";
import useDragAndDrop from "@/scripts/useDnD";
import DropzoneBackground from "@/components/DropzoneBackground.vue";
import {fetchDAGDetail, saveDAGToServer} from "@/api/dag";

const props = defineProps({
  dagId: {type: String, default: null}
})
const emit = defineEmits(['save-complete'])
const {nodes, edges, addNodes, addEdges, onConnect, setNodes, setEdges, updateNode} = useVueFlow()

const {onDragOver, onDrop, onDragLeave, isDragOver, resetId} = useDragAndDrop()
const dagName = ref("")
const dagDescription = ref("Generated DAG")

const selectedNode = ref(null);
const showInputModal = ref(false);

const onNodeClick = (event) => {
  console.log('클릭된 노드:', event.node)
  selectedNode.value = event.node;
  showInputModal.value = true;
}

const saveNodeInputs = (inputs) => {
  console.log(nodes.value)
  console.log(selectedNode.value)
  if (selectedNode.value) {
    updateNode(selectedNode.value.id, (node) => ({
      ...node,
      data: {
        ...node.data,
        inputs: inputs,
      },
    }))
    showInputModal.value = false
  }
}

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
    ...node,
    sourcePosition: 'right',
    targetPosition: 'left',
  })));
  addEdges(dagData.edges.map(edge => ({
    ...edge,
    markerEnd: 'arrowclosed',
  })))
}

watch(() => props.dagId, (newId, oldId) => {
  if (newId && newId !== oldId) {
    loadDag(newId)
  }
})
onConnect((params) => {
  addEdges({
    ...params,
    markerEnd: 'arrowclosed',
  })
})
// DAG 저장
const saveDAG = async () => {
  console.log(nodes, edges)
  const dagData = {
    name: dagName.value,
    description: dagDescription.value,
    nodes: nodes.value.map((node) => ({
      ...node
    })),
    edges: edges.value.map((edge) => ({
      ...edge
    })),
  };

  console.log("🔄 DAG 저장 요청 데이터:", JSON.stringify(dagData, null, 2));

  const response = await saveDAGToServer(dagData);
  if (response && response.message) {
    alert(`✅ DAG 저장 완료: ${response.message}`);
    emit('save-complete');
  } else {
    alert("❌ DAG 저장 실패: 서버 응답이 없습니다.");
  }
};

</script>

<style>
@import "@/scripts/editer.css";
</style>