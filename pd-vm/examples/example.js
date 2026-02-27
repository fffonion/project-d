import { add_one } from "pd-vm-host";

let i = 0;
let total = 0;
while (i < 3) {
    total = total + 1;
    i = i + 1;
}

if (total > 2) {
    console.log(add_one(5));
} else {
    console.log(0);
}
