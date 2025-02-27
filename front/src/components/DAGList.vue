<template>
  <div>
    <h2>DAG 리스트</h2>
    <ul v-if="dags.length > 0">
      <li v-for="dag in dags" :key="dag.id">
        {{ dag.name }} (ID: {{ dag.id }})
      </li>
    </ul>
    <p v-else>등록된 DAG가 없습니다.</p>
  </div>
</template>

<script>
import {onMounted, ref} from "vue";
import {fetchDAGList} from "@/api/dag.js";

export default {
  setup() {
    const dags = ref([]);
    const loadDAGList = async () => {
      dags.value = await fetchDAGList();
    };
    onMounted(loadDAGList);
    return {dags};
  },
};
</script>
