<template>
  <div class="custom-node">
    <Handle type="target" position="left" class="handle-left" />
    <span>{{ label }}</span>
    <Handle type="source" position="right" class="handle-right" />
  </div>
</template>

<script>
import { Handle } from "@vue-flow/core";

export default {
  components: { Handle },
  props: {
    label: String,
  },
};
</script>

<style scoped>
.custom-node {
  border: 2px solid #3498db;
  border-radius: 8px;
  padding: 5px;
  background-color: white;
  width: 150px;
  height: 50px;
  text-align: center;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
}

/* 핸들 스타일 추가 */
.handle-left {
  left: -5px;
  position: absolute;
  width: 10px;
  height: 10px;
  background: #3498db;
  border-radius: 50%;
}

.handle-right {
  right: -5px;
  position: absolute;
  width: 10px;
  height: 10px;
  background: #3498db;
  border-radius: 50%;
}
</style>