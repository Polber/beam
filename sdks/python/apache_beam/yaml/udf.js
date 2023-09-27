function uppercaseName(value) {
  const data = JSON.parse(value);
  data.name = data.name.toUpperCase();
  return JSON.stringify(data);
}