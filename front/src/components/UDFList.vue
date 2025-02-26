<!-- components/UDFList.vue -->
<template>
  <div class="udf-list">
    <h3>📌 UDF 목록</h3>
    <ul>
      <li v-for="udf in udfs" :key="udf.id" draggable="true" @dragstart="onDragStart($event, udf)">
        {{ udf.name }}
      </li>
    </ul>
  </div>
</template>

<script>
import { ref, onMounted } from "vue";
import { fetchUDFList } from "@/api/udf";

export default {
  setup() {
    const udfs = ref([]);
    onMounted(async () => {
      udfs.value = await fetchUDFList();
    });

    const onDragStart = (event, udf) => {
      event.dataTransfer.setData("udf", JSON.stringify(udf));
    };

    return { udfs, onDragStart };
  },
};
</script>

<style scoped>
.udf-list {
  border: 1px solid #ddd;
  padding: 10px;
  max-width: 200px;
}
</style>