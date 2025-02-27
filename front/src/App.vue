<template>
  <div id="app">
    <h1>Workflow Manager</h1>
    <DAGList/>
    <UDFUpload/>
    <DAGEditor/>
  </div>
</template>

<script>
import DAGList from "@/components/DAGList.vue";
import UDFUpload from "@/components/UDFUpload.vue";
import DAGEditor from "@/components/DAGEditor.vue";

export default {
  components: {
    DAGEditor,
    UDFUpload,
    DAGList,
  },
};
</script>
