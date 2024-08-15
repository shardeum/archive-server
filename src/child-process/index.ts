process.on('uncaughtException', (error) => {
  console.error('Uncaught Exception in Child Process:', error)
})
