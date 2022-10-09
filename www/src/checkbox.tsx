import React, {Component} from "react";

type Props = { id: string, label: string, checked: boolean, cb: (checked: boolean) => void }

export class Checkbox extends Component<Props, {}> {
    render() {
        const { id, label, checked, cb } = this.props
        return <div id={id} className="sub-control">
            <label>
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
