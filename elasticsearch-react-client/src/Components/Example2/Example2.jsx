import React, { Component } from 'react';
import { ReactiveBase } from '@appbaseio/reactivesearch';
class Example2 extends Component {
  render() {
    return (
      <ReactiveBase
        app="bank"
        host="http://localhost:9200"
      >
        Hello from Reactive Search!
      </ReactiveBase>
    );
  }
}
export default Example2;