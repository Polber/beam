function my_filter(value) {
    const obj = JSON.parse(value);
    return obj.name == "Bob"
}

function my_map(value) {
    const obj = JSON.parse(value);
    obj.name = "Jeff"
    return JSON.stringify(obj);
}