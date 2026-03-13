import { Context, Schema } from 'koishi'
import type {} from '@koishijs/plugin-server'

export const name = 'esp32-adapter'

export const inject = ['server']

export interface Config {}

export const Config: Schema<Config> = Schema.object({})

export function apply(ctx: Context) {
  const logger = ctx.logger('esp32')
  logger.info('已挂载 esp32 设备专用路由...')

  // 挂载 WebSocket 路由
  ctx.server.ws('/esp32', (socket) => {
    logger.info('ESP32 终端已连接')

    // 监听 WebSocket 消息事件
    socket.on('message', (data) => {
      // 区分二进制流与文本指令
      if (Buffer.isBuffer(data)) {
        logger.info(`收到二进制数据块，大小: ${data.length} bytes`)
      } else {
        try {
          const payload = JSON.parse(data.toString())
          logger.info('收到控制指令:', payload)

          // 阶段一验证逻辑：响应 ESP32 的文本测试
          if (payload.action === 'text_test') {
            socket.send(JSON.stringify({
              action: 'reply_text',
              text: `Koishi 服务端已收到: ${payload.text}`
            }))
          }
        } catch (e) {
          logger.warn('无法解析的数据帧:', data.toString())
        }
      }
    })

    // 监听断开连接事件
    socket.on('close', () => {
      logger.info('ESP32 终端已断开连接')
    })
  })
}
