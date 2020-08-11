const { nanoid } = require('nanoid/async')
const Emittery = require('emittery')
const Deferred = require('@gurupras/deferred')

class RedisPubSub {
  constructor (config, prefix, options) {
    if (!options) {
      options = {}
    }
    const { name, Redis = require('redis') } = options
    Object.assign(this, {
      config,
      prefix,
      name
    })
    new Emittery().bindMethods(this)
    this.pub = Redis.createClient(config)
    this.sub = Redis.createClient(config)

    this.ready = new Deferred()
    this.sub.on('psubscribe', async () => {
      this.ready.resolve()
    })

    this.sub.psubscribe(`${prefix}*`)
    this.sub.on('pmessage', async (pattern, channel, message) => {
      const json = JSON.parse(message)
      const { event, data, requestID } = json
      await this.emit('pmessage', { event, data, requestID })
      return this.emit(event, { data, requestID })
    })
  }

  async publish (event, data, ack) {
    await this.ready
    const obj = {
      event,
      data
    }

    const deferred = new Deferred()
    if (ack) {
      if (typeof ack !== 'string') {
        ack = await nanoid()
      }
      obj.requestID = ack
      this.on(ack, function once ({ data }) {
        this.off(ack, once)
        deferred.resolve(data)
      }.bind(this))
    } else {
      deferred.resolve()
    }
    this.pub.publish(this.prefix, JSON.stringify(obj))
    return deferred
  }

  async destroy () {
    await this.sub.punsubscribe()
    await this.sub.quit()
    await this.pub.quit()
  }
}

module.exports = RedisPubSub
