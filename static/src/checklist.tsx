import React, {ReactElement, ReactNode} from "react";
import {Checkbox} from "./checkbox";

const { fromEntries: obj } = Object

type CheckboxData<T> = {
    name: string
    data: T
    checked?: boolean
    disabled?: boolean
}

export function Checklist<T>(
    { label, data, cb, children, }: {
        label: string | ReactNode
        data: CheckboxData<T>[]
        cb: (ts: T[]) => void
        children?: ReactNode
    }
) {
    const state: { [name: string]: { data: T, checked: boolean } } = obj(
        data.map(
            ({ name, data, checked }) =>
                [
                    name,
                    {
                        data,
                        checked: checked || false,
                    }
                ]
        )
    )

    function onChange(e: any) {
        const name = e.target.value
        const checked: boolean = e.target.checked
        const { checked: cur, data: datum } = state[name]
        if (cur == checked) {
            console.warn("Checkbox", name, "already has value", checked)
        } else {
            let newState = {...state}
            newState[name] = { data: datum, checked }
            const checkeds =
                Object
                    .keys(newState)
                    .filter(name => newState[name].checked)
                    .map(name => newState[name].data)
            cb(checkeds)
        }
    }

    const labels = data.map((d) => {
        const { name, disabled } = d
        const checked = state[name].checked
        return (
            <label key={name}>
                <input
                    type="checkbox"
                    name={name}
                    value={name}
                    checked={checked}
                    disabled={disabled}
                    onChange={() => {
                    }}
                />
                {name}
            </label>
        )
    })

    return <div className="control col">
        <div className="control-header">{label}:</div>
        <div onChange={onChange} className="sub-control">{labels}</div>
        {children}
    </div>
}
