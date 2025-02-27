<template>
  <div class="dnd-flow" @drop="onDrop">
    <VueFlow :nodes="nodes" @dragover="onDragOver" @dragleave="onDragLeave">
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
import {ref} from 'vue'
import {useVueFlow, VueFlow} from '@vue-flow/core'
import FlowSideBar from "@/components/FlowSideBar.vue";
import useDragAndDrop from "@/scripts/useDnD";
import DropzoneBackground from "@/components/DropzoneBackground.vue";

const {onConnect, addEdges} = useVueFlow()

const {onDragOver, onDrop, onDragLeave, isDragOver} = useDragAndDrop()

const nodes = ref([])

onConnect(addEdges)
</script>

<style>
@import "@/scripts/editer.css";
</style>