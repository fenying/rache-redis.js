/**
 *  Copyright 2018 Angus.Fenying <fenying@litert.org>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import * as Rache from "@litert/rache";
import * as Redis from "@litert/redis";

class RedisDriver
implements Rache.IDriver {

    private _redisClient: Redis.RedisClient;

    public constructor(redisClient: Redis.RedisClient) {

        this._redisClient = redisClient;
    }

    public async exists(key: string): Promise<boolean | undefined> {

        let result = await this._redisClient.get(key);

        if (!result) {

            return false;
        }

        if (result.length === 0) {

            return undefined;
        }

        return true;
    }

    public async get(key: string): Rache.AsyncNullable<Rache.CacheBody> {

        let result = await this._redisClient.get(key);

        if (!result) {

            return result;
        }

        if (result.length === 0) {

            return undefined;
        }

        return result;
    }

    public async getMulti(
        keys: string[]
    ): Promise<Record<string, Rache.Nullable<Rache.CacheBody>>> {

        let result = await this._redisClient.mGet(keys);

        let ret: Record<string, Rache.Nullable<Rache.CacheBody>> = {};

        for (let key in result) {

            const data = result[key];

            if (!data) {

                ret[key] = null;
            }
            else if (data.length === 0) {

                ret[key] = undefined;
            }
            else {

                ret[key] = data;
            }
        }

        return ret;
    }

    public async set(
        key: string,
        data: Rache.CacheBody,
        ttl: number
    ): Promise<boolean> {

        if (data === undefined) {

            if (ttl > 0) {

                await this._redisClient.setEX(key, "", ttl);
            }
            else {

                await this._redisClient.set(key, "");
            }
        }
        else {

            if (ttl > 0) {

                await this._redisClient.setEX(key, data, ttl);
            }
            else {

                await this._redisClient.set(key, data);
            }
        }

        return true;
    }

    public async setMulti(
        data: Record<string, Rache.CacheBody>,
        ttl: number
    ): Promise<boolean> {

        if (ttl < 1) {

            const caches: Record<string, Buffer | string> = {};

            for (const key in data) {

                const item = data[key];

                if (item === undefined) {

                    caches[key] = "";
                }
                else {

                    caches[key] = item;
                }
            }

            await this._redisClient.mSet(caches);
        }
        else {

            const prs: Array<Promise<void>> = [];

            this._redisClient.startPipeline();

            for (const key in data) {

                const item = data[key];

                prs.push(this._redisClient.setEX(
                    key,
                    item === undefined ? "" : item,
                    ttl
                ));
            }

            this._redisClient.endPipeline();

            await Promise.all(prs);
        }

        return true;
    }

    public async remove(key: string): Promise<boolean> {

        await this._redisClient.del(key);

        return true;
    }

    public removeMulti(keys: string[]): Promise<number> {

        return this._redisClient.del(...keys);
    }

    public usable(): boolean {

        return this._redisClient.status === Redis.ClientStatus.NORMAL;
    }
}

export function createRedisDriver(client: Redis.RedisClient): Rache.IDriver {

    return new RedisDriver(client);
}

export default createRedisDriver;
