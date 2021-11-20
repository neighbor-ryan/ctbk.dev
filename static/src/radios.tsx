import React, {Component, ReactElement} from "react";

type Option<T> = {
    label?: string
    data: T
    disabled?: boolean
}

type Props<T extends string> = {
    label: string
    options: (Option<T> | T)[]
    choice: T
    cb: (choice: T) => void
    extra?: ReactElement<any, any>
}

type State<T extends string> = { choice: T }

export class Radios<T extends string> extends Component<Props<T>, State<T>> {
    constructor(props: Props<T>) {
        super(props);
        this.state = { choice: props.choice }
        this.onChange = this.onChange.bind(this)
    }
    onChange(e: any) {
        const choice = e.target.value;
        console.log("new choice:", choice)
        this.setState({ choice });
        this.props.cb(choice)
    }
    render() {
        const [ { label, options, extra }, { choice } ] = [ this.props, this.state ]
        const labels = options.map((option) => {
            const { label: text, data: name, disabled } =
                typeof option === 'string'
                    ? { label: option, data: option, disabled: false }
                    : option
            return <label key={name}>
                <input
                    type="radio"
                    name={label + '-' + name}
                    value={name}
                    checked={name == choice}
                    disabled={disabled}
                    onChange={e => {}}
                ></input>
                {text}
            </label>
        })
        return <div className="control col">
            <div className="control-header">{label}:</div>
            <div id={label} className="sub-control" onChange={this.onChange}>{labels}</div>
            {extra}
        </div>
    }
}
