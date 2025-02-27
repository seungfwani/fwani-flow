<template>
  <aside>
    <div class="description">You can drag these nodes to the pane.</div>

<!--    <div class="nodes">-->
<!--      <div class="vue-flow__node-input" :draggable="true" @dragstart="onDragStart($event, 'input')">Input Node</div>-->

<!--      <div class="vue-flow__node-default" :draggable="true" @dragstart="onDragStart($event, 'default')">Default Node-->
<!--      </div>-->

<!--      <div class="vue-flow__node-output" :draggable="true" @dragstart="onDragStart($event, 'output')">Output Node</div>-->
<!--    </div>-->
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