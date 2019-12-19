export function safeParse<Type>(
  fallback: Type,
  json: string,
  msg?: string
): Type {
  try {
    return JSON.parse(json)
  } catch (err) {
    console.warn(msg ? msg : err)
    return fallback
  }
}
