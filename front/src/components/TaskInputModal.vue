<template>
  <div class="modal-overlay" v-if="visible">
    <div class="modal-content">
      <h3>노드 입력값 설정 ({{ node?.data.label }})</h3>
      <div v-for="(item, index) in inputs" :key="index" style="margin-bottom: 8px;">
        <label>Key:</label>
        <input v-model="item.key" placeholder="Key"/>
        <label>Value:</label>
        <input v-model="item.value" placeholder="Value"/>
        <button @click="removeField(index)" style="margin-left: 6px;">❌</button>
      </div>
      <button @click="addInputField">필드 추가</button>
      <br/><br/>
      <button @click="save">저장</button>
      <button @click="close">닫기</button>
    </div>
  </div>
</template>

<script setup>
import {defineEmits, defineProps, ref, watch} from 'vue'

const props = defineProps({
  visible: Boolean,
  node: Object,
})

const emit = defineEmits(['close', 'save'])

const inputs = ref([])

watch(() => props.node, (newNode) => {
  if (newNode) {
    inputs.value = Object.entries(newNode.data?.inputs || {}).map(([key, value]) => ({
      key,
      value,
    }))
  }
})

const addInputField = () => {
  inputs.value.push({key: '', value: ''})
}
const removeField = (index) => {
  inputs.value.splice(index, 1)
}

const save = () => {
  const inputObject = {}
  inputs.value.forEach((item) => {
    if (item.key.trim() !== '') {
      inputObject[item.key] = item.value
    }
  })
  emit('save', inputObject)
}

const close = () => {
  emit('close')
}
</script>

<style scoped>
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
}

.modal-content {
  position: absolute;
  top: 30%;
  left: 50%;
  transform: translate(-50%, -30%);
  background: #fff;
  padding: 20px;
  width: 400px;
  border-radius: 8px;
}

.modal-content input {
  margin-right: 8px;
  margin-bottom: 4px;
}
</style>