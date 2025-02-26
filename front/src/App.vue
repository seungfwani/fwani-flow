<template>
  <div id="app">
    <h1>Workflow Manager</h1>
    <DAGList/>
    <UDFList/>
    <UDFUpload/>
    <DAGEditor/>
  </div>
</template>

<script>
import DAGList from "@/components/DAGList.vue";
import UDFList from "@/components/UDFList.vue";
import UDFUpload from "@/components/UDFUpload.vue";
import DAGEditor from "@/components/DAGEditor.vue";

export default {
  components: {
    DAGEditor,
    UDFUpload,
    DAGList,
    UDFList,
  },
};
</script>
