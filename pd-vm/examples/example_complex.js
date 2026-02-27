import * as string from "../stdlib/rss/strings.rss";
import { add_one } from "pd-vm-host";

// Complex JavaScript flavor example: loop + stdlib + host + closure.
let total = 0;
for (let i = 0; i < 4; i = i + 1) {
    total = total + i;
}

if (!string.non_empty("javascript")) {
    total = 0;
} else {
    total = add_one(total);
}

let base = 7;
let add = (value) => value + base;
base = 8;
let closureValue = add(5);

console.log(closureValue);
