<template>
  <div>
    <h2>DAG 리스트</h2>
    <ul v-if="dags.length > 0">
      <li v-for="dag in dags" :key="dag.id">
        <button @click="$emit('select-dag', dag.id)">
          {{ dag.name }} (ID: {{ dag.id }})
        </button>
      </li>
    </ul>
    <p v-else>등록된 DAG가 없습니다.</p>
  </div>
</template>

<script setup>
import {onMounted, ref} from "vue";
import {fetchDAGList} from "@/api/dag.js";

const dags = ref([]);

onMounted(async () => {
  dags.value = await fetchDAGList();
});
</script>
