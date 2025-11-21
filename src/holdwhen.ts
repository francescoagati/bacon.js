import { EventStream, default as Observable, Property } from "./observable";
import { Event, endEvent, nextEvent, hasValue } from "./event";
import CompositeUnsubscribe from "./compositeunsubscribe";
import { Desc } from "./describe";
import { EventSink, Unsub } from "./types";
import { Reply, more, noMore } from "./reply";

/** @hidden */
export function holdWhen<V>(src: Observable<V>, valve: Property<boolean>): EventStream<V> {
  let onHold = false
  let bufferedValues: V[] = []
  let srcIsEnded = false
  return new EventStream(new Desc(src, "holdWhen", [valve]), (sink: EventSink<V>) => {
    const composite = new CompositeUnsubscribe()
    let subscribed = false
    const flushBufferedValues = (): Reply => {
      for (const value of bufferedValues) {
        const reply = sink(nextEvent(value))
        if (reply === noMore) {
          bufferedValues = []
          return noMore
        }
      }
      bufferedValues = []
      return more
    }
    const endIfBothEnded = (unsub?: Unsub): Reply => {
      if (unsub) { unsub() }
      if (composite.empty() && subscribed) {
        return sink(endEvent())
      }
      return more
    }
    composite.add((_unsubAll: Unsub, unsubMe: Unsub) => {
      return valve.subscribeInternal((event: Event<boolean>): Reply => {
        if (hasValue(event)) {
          onHold = event.value
          if (!onHold) {
            const result = flushBufferedValues()
            if (result === noMore) { return noMore }
            if (srcIsEnded) {
              unsubMe()
              return sink(endEvent())
            }
          }
          return more
        } else if (event.isEnd) {
          return endIfBothEnded(unsubMe)
        } else {
          return sink(<any>event)
        }
      })
    })
    composite.add((_unsubAll: Unsub, unsubMe: Unsub) => {
      return src.subscribeInternal((event: Event<V>): Reply => {
        if (onHold && hasValue(event)) {
          bufferedValues.push(event.value)
          return more
        } else if (event.isEnd && bufferedValues.length) {
          srcIsEnded = true
          return endIfBothEnded(unsubMe)
        } else {
          return sink(event)
        }
      })
    })
    subscribed = true
    endIfBothEnded()
    return composite.unsubscribe
  });
}
