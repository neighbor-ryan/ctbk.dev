import React, {Component} from "react";

type Props<T extends string> = {
    label: string
    options: T[]
    choice: T
    cb: (choice: T) => void
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
        const [ { label, options }, { choice } ] = [ this.props, this.state ]
        const labels = options.map((name) => {
            return <label key={name}>
                <input
                    type="radio"
                    name={label + '-' + name}
                    value={name}
                    checked={name == choice}
                    onChange={e => {}}
                ></input>
                {name}
            </label>
        })
        return <div className="control col">
            <div className="control-header">{label}:</div>
            <div id={label} onChange={this.onChange}>{labels}</div>
        </div>
    }
}
