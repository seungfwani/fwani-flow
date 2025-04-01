<template>
  <UDFUpload/>
  <aside>
    <div class="description">아래 함수를 Drag&Drop 하여 DAG 를 구성해보세요.</div>
    <!-- UDF 목록 -->
    <div class="nodes">
      <div
          v-for="udf in udfList"
          :key="udf.id"
          :title="udf.name"
          class="vue-flow__node-default"
          draggable="true"
          @dragstart="onDragStart($event, 'default', udf)"
      >
        {{ udf.name }}
      </div>
    </div>
  </aside>
</template>

<script setup>

import useDragAndDrop from "@/scripts/useDnD";
import {fetchUDFList} from "@/api/udf";
import {onMounted, ref} from "vue";
import UDFUpload from "@/components/UDFUpload.vue";

const {onDragStart} = useDragAndDrop()
const udfList = ref([]);

// UDF 목록 불러오기
const loadUDFs = async () => {
  udfList.value = await fetchUDFList();
};

onMounted(loadUDFs);
</script>

<style>
@import "@/scripts/editer.css";
</style>