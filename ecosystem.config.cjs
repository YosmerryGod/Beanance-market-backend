module.exports = {
  apps: [
    {
      name: "beanance-market",
      script: "src/index.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "512M",
      node_args: "--max-old-space-size=384",
      restart_delay: 3000,
      min_uptime: "10s",
      max_restarts: 10,
      exp_backoff_restart_delay: 1000,
      time: true,
      env: {
        NODE_ENV: "production",
        PORT: 3030,
        LOG_LEVEL: "warn"
      }
    }
    ,
    {
      name: "beanance-market-mcp",
      script: "src/mcp-server.js",
      cwd: __dirname,
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      watch: false,
      max_memory_restart: "256M",
      node_args: "--max-old-space-size=192",
      restart_delay: 3000,
      min_uptime: "10s",
      max_restarts: 10,
      exp_backoff_restart_delay: 1000,
      time: true,
      env: {
        NODE_ENV: "production",
        MCP_HOST: "127.0.0.1",
        MCP_PORT: 3031,
        LOG_LEVEL: "warn"
      }
    }
  ]
};
