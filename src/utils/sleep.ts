export default function sleep(ms: number) {
  return new Promise(function (resolve) {
    return setTimeout(resolve, ms);
  });
}
