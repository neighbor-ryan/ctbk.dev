import Config from "next/config"

export const basePath = Config()?.publicRuntimeConfig?.basePath || ""
