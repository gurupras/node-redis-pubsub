import Redis from 'redis-mock'
import { nanoid } from 'nanoid'

import Deferred from '@gurupras/deferred'
import { testForEvent } from '@gurupras/test-helpers'
import RedisPubSub from '../index'

let prefix
beforeEach(() => {
  prefix = `redis-pubsub-test:${nanoid()}`
})

describe('RedisPubSub', () => {
  let client1
  let client2
  let data
  let event
  beforeEach(async () => {
    event = `test-event:${nanoid()}`
    client1 = new RedisPubSub(null, prefix, { name: 'client1', Redis })
    client2 = new RedisPubSub(null, prefix, { name: 'client2', Redis })
    await client1.ready
    await client2.ready
    data = {
      now: Date.now(),
      deep: {
        nested: {
          object: {
            value: 4
          }
        }
      }
    }
  })

  afterEach(async () => {
    await client1.destroy()
    await client2.destroy()
  })

  test('Basic pubsub works', async () => {
    const promise = testForEvent(client2, event, { timeout: 300 })
    client1.publish(event, data)
    await expect(promise).toResolve()
    const { data: got } = await promise
    expect(got).toEqual(data)
  })

  test('Passing no options works as expected', async () => {
    // This will throw due to invalid config. Check for that
    const client = new RedisPubSub({ host: 'bad' }, 'test')

    const promises = [
      testForEvent(client.pub, 'error', { timeout: 300 }),
      testForEvent(client.sub, 'error', { timeout: 300 })
    ]
    await expect(Promise.all(promises)).toResolve()
    return client.destroy()
  })

  async function testAck (ack) {
    const response = { deep: { object: { value: ['test'] } } }
    client2.on(event, function once ({ data, requestID }) {
      client2.off(event, once)

      client2.publish(requestID, response)
    })
    await expect(client1.publish(event, data, ack)).resolves.toEqual(response)
  }

  test.each([
    true,
    `${event}-response`
  ])('Specifying ack(%p) retrieves value', async (ack) => {
    await expect(testAck(ack)).toResolve()
  })
})
