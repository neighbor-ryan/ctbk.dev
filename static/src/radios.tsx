import React, {ReactNode} from "react";

type Option<T> = {
    label?: string | ReactNode
    data: T
    disabled?: boolean
}

export function Radios<T extends string>(
    { label, options, choice, cb, children }: {
        label: string
        options: (Option<T> | T)[]
        choice: T
        cb: (choice: T) => void
        children?: ReactNode
    }
) {
    const labels = options.map(option => {
        const { label: text, data: name, disabled } =
            typeof option === 'string'
                ? { label: option, data: option, disabled: false }
                : option
        return (
            <label key={name}>
                <input
                    type="radio"
                    name={label + '-' + name}
                    value={name}
                    checked={name == choice}
                    disabled={disabled}
                    onChange={e => {
                    }}
                />
                {text}
            </label>
        )
    })
    return <div className="control col">
        <div className="control-header">{label}:</div>
        <div id={label} className="sub-control" onChange={(e: any) => cb(e.target.value)}>{labels}</div>
        {children}
    </div>
}
