import 'bootstrap/dist/css/bootstrap.css'
import '../styles/globals.css'
import { AppProps } from "next/app"
import { ThemeProvider } from "@mui/material"
import theme from "../src/theme"

export default function App({ Component, pageProps }: AppProps) {
  return (
    <ThemeProvider theme={theme}>
      <Component {...pageProps} />
    </ThemeProvider>
  )
}
