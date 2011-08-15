/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
module thrift.util.resource_pool;

import core.time : Duration, dur, TickDuration;
import std.algorithm : minPos, reduce, remove;
import std.array : array;
import std.exception : enforce;
import std.conv : to;
import std.random : randomCover, rndGen;
import std.range : zip;

/**
 * A pool of resources, which can be iterated over, and where resources that
 * have failed too often can be temporarily disabled.
 *
 * This class is oblivious to the actual resource type managed.
 */
final class TResourcePool(Resource) {
  /**
   * Constructs a new instance.
   *
   * Params:
   *   resources = The initial members of the pool.
   */
  this(Resource[] resources) {
    resources_ = resources;
  }

  /**
   * Adds a resource to the pool.
   */
  void add(Resource resource) {
    resources_ ~= resource;
  }

  /**
   * Removes a resource from the pool.
   *
   * Returns: Whether the resource could be found in the pool.
   */
  bool remove(Resource resource) {
    auto removed = .remove!((a){ return a !is resource; })(resources_).length;
    resources_ = resources_[0 .. $ - removed];
    return (removed > 0);
  }

  /**
   * Returns an »enriched« input range to iterate over the pool members.
   */
  struct Range {
    /**
     * Whether the range is empty.
     *
     * This is the case if all members of the pool have failed and
     * TResourcePool.cycle is false, or all the pool members are temporarily
     * disabled because they have failed too often.
     */
    bool empty() @property {
      if (cached_) return false;

      if (nextIndex_ == resources_.length) {
        if (!parent_.cycle) return true;
        nextIndex_ = 0;
      }

      foreach (i; 0 .. resources_.length) {
        auto r = resources_[(i + nextIndex_) % resources_.length];
        auto fi = r in parent_.faultInfos_;

        if (fi && fi.resetTime != fi.resetTime.init) {
          // The argument to < needs to be an lvalue…
          auto currentTick = TickDuration.currSystemTick;
          if (fi.resetTime < currentTick) {
            // The timeout expired, remove the resource from the list and go
            // ahead trying it.
            parent_.faultInfos_.remove(r);
          } else {
            // The timeout didn't expire yet, try the next resource.
            continue;
          }
        }

        cache_ = r;
        cached_ = true;
        nextIndex_ = i;
        return false;
      }

      // If we get here, all resources are currently inactive, so there is
      // nothing we can do.
      return true;
    }

    /**
     * Returns the first resource in the range.
     */
    Resource front() @property {
      enforce(!empty);
      return cache_;
    }

    /**
     * Removes the first resource from the range.
     *
     * Usually, this is combined with a call to TResourcePool.recordSuccess()
     * or recordFault().
     */
    void popFront() {
      enforce(!empty);
      cached_ = false;
    }

    /**
     * Returns whether the range will become non-empty at some point in the
     * future, and provides additional information when this will happen and
     * what will be the next resource.
     *
     * Params:
     *   next = The next resource that will become available.
     *   waitTime = The duration until that resource will become available.
     */
    bool willBecomeNonempty(out Resource next, out Duration waitTime) {
      assert(empty);
      if (!parent_.cycle) return false;

      auto fi = parent_.faultInfos_;
      auto nextPair = minPos!"a[1].resetTime < b[1].resetTime"(
        zip(fi.keys, fi.values)
      ).front;

      next = nextPair[0];
      waitTime = to!Duration(TickDuration.currSystemTick - nextPair[1].resetTime);

      return true;
    }

  private:
    this(TResourcePool parent, Resource[] resources) {
      parent_ = parent;
      resources_ = resources;
    }

    TResourcePool parent_;
    Resource[] resources_;
    Resource cache_;
    bool cached_;
    size_t nextIndex_;
  }

  /// Ditto
  Range opSlice() {
    auto res = resources_;
    if (permute) {
      res = array(randomCover(res, rndGen));
    }
    return Range(this, res);
  }

  /**
   * Records a success for an operation on the given resource, cancelling a
   * fault streak, if any.
   */
  void recordSuccess(Resource resource) {
    if (resource in faultInfos_) {
      faultInfos_.remove(resource);
    }
  }

  /**
   * Records a fault for the given resource.
   *
   * If a resource fails consecutively for more than faultDisableCount times,
   * it is temporarily disabled (no longer considered) until
   * faultDisableDuration has passed.
   */
  void recordFault(Resource resource) {
    auto fi = resource in faultInfos_;

    if (!fi) {
      faultInfos_[resource] = FaultInfo();
      fi = resource in faultInfos_;
    }

    ++fi.count;
    if (fi.count >= faultDisableCount) {
      // If the resource has hit the fault count limit, disable it for
      // specified duration.
      fi.resetTime = TickDuration.currSystemTick +
        cast(TickDuration)faultDisableDuration;
    }
  }

  /**
   * Whether to randomly permute the order of the resources in the pool when
   * taking a range using opSlice().
   *
   * This can be used e.g. as a simple form of load balancing.
   */
  bool permute = true;

  /**
   * Whether to keep iterating over the pool members after all have been
   * returned/have failed once.
   */
  bool cycle = false;

  /**
   * The number of consecutive faults after which a resource is disabled until
   * faultDisableDuration has passed. Zero to never disable resources.
   *
   * Defaults to zero.
   */
  ushort faultDisableCount = 0;

  /**
   * The duration for which a resource is no longer considered after it has
   * failed too often.
   *
   * Defaults to one second.
   */
  Duration faultDisableDuration = dur!"seconds"(1);

private:
  Resource[] resources_;
  FaultInfo[Resource] faultInfos_;
}

private {
  struct FaultInfo {
    ushort count;
    TickDuration resetTime;
  }
}
