import { add_one } from "pd-vm-host";

// Example: all core syntax in JavaScript flavor
let sum = 0;
let limit = 5;

for (let i = 0; i < limit; i = i + 1) {
    if (i == 1) {
        continue;
    }
    if (i > 2) {
        break;
    }
    sum = sum + i;
}

let j = 0;
while (j < 4) {
    j = j + 1;
    if (j == 2) {
        continue;
    }
    sum = sum + 1;
    if (j > 2) {
        break;
    }
}

let bump = 1;
let adjust = (value) => value + bump;
bump = 100;

if (sum > 0) {
    sum = adjust(sum);
} else {
    sum = 0;
}

console.log(add_one(sum));
