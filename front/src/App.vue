<template>
  <div id="app">
    <h1>Workflow Manager</h1>
    <DAGList @select-dag="handleSelectDag"/>
    <UDFUpload/>
    <DAGEditor :dag-id="selectedDagId"/>
  </div>
</template>

<script setup>
import DAGList from "@/components/DAGList.vue";
import UDFUpload from "@/components/UDFUpload.vue";
import DAGEditor from "@/components/DAGEditor.vue";
import {ref} from "vue";

const selectedDagId = ref(null);
const handleSelectDag = (id) => {
  console.log("DAG 선택됨: ", id);
  selectedDagId.value = id;
}
</script>
