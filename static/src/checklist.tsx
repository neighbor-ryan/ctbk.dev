import React, {Component} from "react";

type CheckboxData<T> = {
    name: string
    data: T
    checked?: boolean
    disabled?: boolean
}

type ChecklistProps<T> = {
    label: string
    data: CheckboxData<T>[]
    cb: (ts: T[]) => void
}
type ChecklistState<T> = { [key: string]: { data: T, checked: boolean } }


export class Checklist<T> extends Component<ChecklistProps<T>, ChecklistState<T>> {
    constructor(props: ChecklistProps<T>) {
        super(props);
        let obj: { [key: string]: { data: T, checked: boolean } } = {}
        props.data.forEach((d) => obj[d.name] = { data: d.data, checked: d.checked || false })
        this.state = obj
    }

    render() {
        const [ { label, data, cb }, state ] = [ this.props, this.state ]

        let onChange = function(this: Checklist<T>, e: any) {
            let newState = {...state}
            const name = e.target.value
            const checked: boolean = e.target.checked
            // console.log("target:", name, "checked:", checked)
            const { checked: cur, data: elem } = state[name]
            if (cur == checked) {
                console.warn("Checkbox", name, "already has value", checked)
            } else {
                newState[name] = { data: elem, checked }
                // console.log("setState", newState)
                this.setState(newState)
                const elems =
                    Object
                        .keys(newState)
                        .filter((name) => newState[name].checked)
                        .map((name) => newState[name].data)
                cb(elems)
            }
        }
        onChange = onChange.bind(this)

        const labels = data.map((d) => {
            const { name, disabled } = d
            const checked = state[name].checked
            return <label key={name}>
                <input
                    type="checkbox"
                    name={name}
                    value={name}
                    checked={checked}
                    disabled={disabled}
                    onChange={e => {}}
                ></input>
                {name}
            </label>
        })

        return <div className="control col">
            <div className="control-header">{label}:</div>
            <div id={label} onChange={onChange}>{labels}</div>
        </div>
    }
}
