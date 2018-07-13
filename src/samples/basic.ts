// tslint:disable:no-console

import * as Rache from "@litert/rache";
import * as Redis from "@litert/redis";
import createRedisDriver from "../libs";

interface User {

    id: number;

    name: string;

    email: string;

    system: number;
}

(async () => {

    /**
     * Create a resource cache management hub.
     */
    const hub = Rache.createHub();

    /**
     * Create a connection to redis server.
     */
    const redis = await Redis.createRedisClient({
        host: "127.0.0.1",
        port: 6677
    });

    /**
     * Authenticate with redis server if necessary.
     */
    await redis.auth("redis-passwd");

    /**
     * Wrap redis connection into a driver, and register into the hub.
     */
    hub.addDriver("redis", createRedisDriver(redis));

    /**
     * Create a zone to manage the cache of Users, using the registered driver.
     */
    const users = hub.createZone<User>(
        "users",
        "redis",
        JSON.stringify,
        <any> JSON.parse
    );

    /**
     * Handle the error when can not access to cache server, or for other
     * reason.
     *
     * If don't listen to this event, a exception will be thrown at the calling
     * point.
     */
    users.on("error", function(e) {

        console.error(e);
    });

    /**
     * Register a single-key entry for User cache.
     */
    users.registerEntry("primary", {"id": "number"});

    /**
     * Register a multi-keys entry for User cache.
     */
    users.registerEntry("email", {
        "email": "string",
        "system": "number"
    });

    /**
     * Register a multi-keys entry for User cache.
     */
    users.registerEntry("name", {
        "name": "string",
        "system": "number"
    });

    /**
     * Register an attachment for User cache, using the entry "primary".
     */
    users.registerAttachment(
        "roles",
        "primary",
        JSON.stringify,
        <any> JSON.parse, // JSON.parse can parse JSOn text even in a Buffer.
        3600
    );

    /**
     * Register an attachment for User cache, using the entry "primary".
     */
    users.registerAttachment(
        "wallet",
        "primary",
        JSON.stringify,
        <any> JSON.parse,
        3600
    );

    const theUser = {
        id: 1,
        name: "hello",
        email: "user@sample.com",
        system: 33
    };

    /**
     * Write the user into cache, with all the entries registered for User.
     */
    await users.put(theUser);

    console.log(await users.read("primary", { id: 1 }));
    console.log(await users.read("email", {
        email: "user@sample.com",
        system: 33
    }));

    /**
     * Mark an entry of a User as NEVER-EXISTED, so you will get a symbol
     * "NO_DATA", when you try to fetch the User by the entry.
     *
     * This helps avoiding the attack of missing cache.
     */
    await users.markNeverExist("primary", { id: 2 });
    await users.markNeverExist("email", {
        email: "hacker@sample.com",
        system: 31
    });

    await users.markMultiNeverExist("primary", [
        {"id": 444 },
        {"id": 555 }
    ]);

    /**
     * The email "hacker@sample.com" in system 31 has been mark as
     * NEVER-EXISTED, so here it will be a NO_DATA symbol as return value.
     */
    console.log(await users.read("email", {
        email: "hacker@sample.com",
        system: 31
    }));

    console.log(await users.read("primary", {id: 2}));
    console.log(await users.read("primary", {id: 555}));

    /**
     * The user of ID 333 is never written into cache, so it will be a NULL.
     */
    console.log(await users.read("primary", {id: 333}));

    console.log(await users.readAttachment("roles", { id: 1 }));

    console.log(await users.writeAttachment("wallet", { id: 1 }, 0));

    console.log(await users.readAttachment("wallet", { id: 1 }));

    console.log(await users.writeAttachment("roles", { id: 1 }, [1, 2, 3]));

    console.log(await users.readAttachment("roles", { id: 1 }));

    console.log(await users.removeAttachment("roles", { id: 1 }));

    console.log(await users.readAttachment("roles", { id: 1 }));

    console.log(await users.writeAttachment("roles", { id: 1 }, [1, 2, 3]));

    console.log(await users.removeAllAttachments(theUser));

    console.log(await users.readAttachment("roles", theUser));

    console.log(await users.readAttachment("wallet", theUser));

    await users.flush(theUser);

    await users.put(theUser);

    console.log(await users.writeAttachment("wallet", { id: 1 }, 0));

    console.log(await users.writeAttachment("roles", { id: 1 }, [1, 2, 3]));

    await users.flush(theUser, true);

    await redis.close();

})().catch((e) => {

    console.error(e.message);
    console.error(e.stack);
});
