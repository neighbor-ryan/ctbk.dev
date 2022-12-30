import React, {Component} from "react";
import css from "./controls.module.css"

type Props = {
    id: string
    label: string
    checked: boolean
    nowrap?: boolean
    cb: (checked: boolean) => void
}

export class Checkbox extends Component<Props, {}> {
    render() {
        const { id, label, checked, nowrap = true, cb } = this.props
        return <div id={id} className={css.subControl}>
            <label className={nowrap ? css.nowrap : ""}>
                <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => cb(e.target.checked)}
                />
                {label}
            </label>
        </div>
    }
}
